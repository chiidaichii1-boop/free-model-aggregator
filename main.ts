// free-model-aggregator-02's entry: one server, two client wires, and a real free model behind
// every answer.
//
// The server listens on 0.0.0.0 at a free port, registers that port and its catalog with the
// trial's env, and answers each prompt the env sends with a real completion from one of the free
// models the brief lists, dialled through the grader's gateway on the trial's key. It never
// answers a prompt with a completion of its own.
//
//   A trial runs this server as `node /bot/main.ts` from your repository's root, with Node 22's
//   standard library alone (nothing is installed; TypeScript runs directly, and a relative import
//   needs its .ts extension). The env is at $MAKEFUN_ENV_URL; model calls go through the grader's
//   gateway at $OPENAI_BASE_URL, an OpenAI-compatible endpoint that reaches OpenRouter on the key
//   pushed to the competition. The brief at $MAKEFUN_PROMPT_FILE carries two documents:
//     - the free model list: each free OpenRouter model's gateway id
//       (`openrouter/<vendor>/<model>:free`), its published rating in milli-points, what it can do
//       (tools, vision, JSON mode, context length) and its free flag;
//     - the six prompts: each one's id, its api (openai or anthropic), whether it streams, its
//       weight (1 or 2) and the requirements a model must meet to serve it. Each prompt carries a
//       unique token the answer must contain.
//   The server:
//     1. listens on 0.0.0.0 at a free port (never 8080: the env itself is there on one layout);
//     2. POSTs $MAKEFUN_ENV_URL/register {"protocol_version": 1, "port": <port>, "catalog": <catalog>};
//     3. serves GET /v1/models (answering in the shape of the auth header it got: x-api-key means
//        Anthropic, Authorization means OpenAI), POST /v1/chat/completions and POST /v1/messages,
//        each with a JSON body or with server-sent events;
//     4. answers each prompt by dialling ONE free model through the gateway. OPENAI_API_KEY in the
//        sandbox is a dummy the gateway swaps for the OpenRouter key pushed to the competition;
//        the real key never enters the sandbox;
//     5. polls GET $MAKEFUN_ENV_URL/work every 100 ms and exits 0 when the env says {"done": true}.
//        A process still running when the wall ends scores zero, so never wait for anything else.
//   `node main.ts --list` prints the same catalog JSON and exits 0 before any server starts; the
//   register body's catalog is that same document, byte for byte.
//
// THE MODEL RULE
//   Whatever this server reports in a response's `model` field must be the model it actually
//   dialled. The house records every gateway call and scores the model that really answered. This
//   server sends the brief's model id in the gateway request and puts that same id back on every
//   answer and every streamed chunk (the provider's own answer spells it without the gateway's
//   `openrouter/` prefix), so its claim is the call it made, never another. A prompt whose reported
//   model has no matching successful call in the house's record earns nothing.
//
// ROUTING (quality is 60% of the score)
//   For each prompt the server ranks the free models that meet the prompt's requirements by the
//   brief's rating_milli, highest first, and dials the best. A JSON-mode prompt goes to the best
//   JSON-capable model, a tools prompt to the best tools-capable one, and an unconstrained prompt
//   to the best model overall. The ratings come from the brief at runtime, never from a hardcoded
//   list: the scored instances are drawn after entries close and carry their own briefs.
//
// FAILOVER (10% of the score) AND COOLDOWNS
//   A gateway call answered with a 4xx or a 5xx is not an answer. The server retries a 5xx or a
//   broken connection once on the same model, then fails over to the next-best free model that
//   meets the prompt; a 429 cools that model down for the rest of the trial, so one rate-limited
//   model never takes every later prompt down with it. Every refusal is reported in the response
//   header `x-makefun-upstream-failures` (at most 8 rows), the only way the failover column can
//   see a refusal the server retried and then served. The tie-break is the number of gateway
//   calls, so attempts are capped: at most four per prompt.
//
// THE CATALOG (5% of the score)
//   The catalog lists every free model the brief names, exactly the fourteen ids this column
//   scores. The same set is registered and answered by GET /v1/models on both auth headers.
//
// DETERMINISM
//   Every final trial runs twice (live, then replayed from the recorded gateway answers); the
//   replay must leave the same state export byte for byte. So no choice here reads the wall
//   clock (no Date.now, no new Date in a request) and none reads unseeded randomness (no
//   Math.random, no random UUID): routing is a pure function of the brief and the refusals seen
//   so far, and the env is asked one request at a time, in the order they were made.
//
// FILES
//   main.ts            this server: the brief, the catalog, the routes, the register and the poll.
//   lib/providers.ts   the brief's types and parser, the free model list, the catalog.
//   lib/wire.ts        Anthropic <-> Chat Completions translation, the model field, the SSE writers.
//   LICENSE            MIT: keep it or replace it with another OSI-approved license.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import {
  buildCatalog,
  capabilitiesOf,
  freeModels,
  isRecord,
  meetsRequirements,
  rankedCandidates,
  readBrief,
  type CatalogReport,
  type Json,
  type ModelRow,
  type Requirement,
} from './lib/providers.ts';
import {
  anthropicError,
  anthropicEventsFromOpenAI,
  anthropicModelList,
  anthropicToOpenAI,
  openAIError,
  openAIJsonWithModel,
  openAIModelList,
  openAIStreamWithModel,
  openAIToAnthropic,
  type ServedModel,
} from './lib/wire.ts';

/** The wire protocol's version, in the register body. */
const PROTOCOL_VERSION = 1;
/** How often GET /work is polled, in milliseconds. */
const POLL_INTERVAL_MS = 100;
/** The most bytes of one client request body this server reads. */
const MAX_BODY_BYTES = 8 * 1024 * 1024;
/** The most gateway attempts one prompt costs (the tie-break counts calls, so keep it tight). */
const MAX_ATTEMPTS_PER_PROMPT = 4;
/** The most rows the upstream-failures header carries. */
const MAX_TRAIL_ROWS = 8;
/** The pause before one retry of a 5xx or a broken connection, in milliseconds. */
const RETRY_PAUSE_MS = 300;
/** The output cap a request body gets when it names none (the gateway refuses an uncapped call). */
const DEFAULT_MAX_TOKENS = 1024;
/** The gateway's per-call output cap. */
const GATEWAY_MAX_TOKENS = 8192;

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const PROMPT_FILE = process.env.MAKEFUN_PROMPT_FILE ?? '';
const GATEWAY_URL = (process.env.OPENAI_BASE_URL ?? '').replace(/\/+$/, '');
/** The trial's dummy key for the entry's gateway lane; the gateway swaps it for the entrant's own key. */
const GATEWAY_KEY = process.env.OPENAI_API_KEY ?? '';

const brief = readBrief(PROMPT_FILE);
const catalog: CatalogReport = buildCatalog(brief);
/** Which prompt row the request now arriving is: the brief's prompts are in the client's arrival order. */
let manifestIndex = 0;
/** Models a refusal has cooled down for the rest of the trial, by id. */
const coolingDown = new Set<string>();

function log(message: string): void {
  process.stderr.write(`aggregator: ${message}\n`);
}

/** One row of the upstream-failures trail the server reports to the client. */
interface FailureRow {
  status: number;
  code: string;
  model: string | null;
}

/** Pause for `ms` milliseconds; a backoff, never a clock reading in a request. */
function pause(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * The gateway's Chat Completions route. OPENAI_BASE_URL is the OpenAI-compatible root (`…/v1` on
 * the trial network); a base that does not already end in `/v1` gets one, so both spellings reach
 * the route.
 */
function chatCompletionsUrl(): string {
  if (GATEWAY_URL === '') return '';
  return GATEWAY_URL.endsWith('/v1') ? `${GATEWAY_URL}/chat/completions` : `${GATEWAY_URL}/v1/chat/completions`;
}

/** The free models the brief lists, as GET /v1/models names them. */
function servedModels(): ServedModel[] {
  return freeModels(brief).map((model) => ({ id: model.id, provider: model.provider, display_name: model.display_name }));
}

/** The requirements of the prompt now arriving: the brief's row by arrival order, else the body's own. */
function requirementsFor(body: Json): Requirement {
  const entry = brief.prompts[manifestIndex];
  manifestIndex += 1;
  if (entry !== undefined) return entry.requires;
  // The brief's prompt list is the declared source of each request's requirements; this fallback
  // is a guess for a brief that holds none.
  return requirementsFromBody(body);
}

/** Requirements read off a request body, when the brief has no row for it. */
function requirementsFromBody(body: Json): Requirement {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  let vision = false;
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (isRecord(block) && (block.type === 'image' || block.type === 'image_url')) vision = true;
    }
  }
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return {
    tools: Array.isArray(body.tools) && body.tools.length > 0,
    vision,
    json: format === 'json_object' || format === 'json_schema',
  };
}

/** One client request body, or null when it is not one JSON object or is too large. */
async function readJson(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buffer);
  }
  if (chunks.length === 0) return null;
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

/** The failures header's value: the trail as JSON, at most eight rows. */
function trailHeader(failures: readonly FailureRow[]): string {
  return JSON.stringify(failures.slice(0, MAX_TRAIL_ROWS));
}

function sendJson(res: ServerResponse, status: number, body: unknown, failures: readonly FailureRow[] = []): void {
  const text = JSON.stringify(body);
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    'content-length': String(Buffer.byteLength(text)),
  };
  if (failures.length > 0) headers['x-makefun-upstream-failures'] = trailHeader(failures);
  res.writeHead(status, headers);
  res.end(text);
}

function sendError(res: ServerResponse, wire: 'openai' | 'anthropic', status: number, type: string, message: string, failures: readonly FailureRow[] = []): void {
  if (wire === 'anthropic') sendJson(res, status, anthropicError(type, message), failures);
  else sendJson(res, status, openAIError(message, type, null), failures);
}

/** GET /v1/models, in the shape of the auth header the client sent. */
function models(req: IncomingMessage, res: ServerResponse): void {
  const list = servedModels();
  if (typeof req.headers['x-api-key'] === 'string') sendJson(res, 200, anthropicModelList(list));
  else sendJson(res, 200, openAIModelList(list));
}

/** The outgoing Chat Completions body: the client's, with the dialled model and an output cap. */
function gatewayBody(body: Json, model: string): Json {
  const out: Json = { ...body, model };
  const capped =
    typeof out.max_tokens === 'number' || typeof out.max_completion_tokens === 'number' || typeof out.max_output_tokens === 'number';
  if (!capped) out.max_tokens = DEFAULT_MAX_TOKENS;
  if (typeof out.max_tokens === 'number' && out.max_tokens > GATEWAY_MAX_TOKENS) out.max_tokens = GATEWAY_MAX_TOKENS;
  if (out.stream === true) out.stream_options = { include_usage: true };
  return out;
}

/** The error code a gateway refusal carries: its error.code, else its error.type, else ''. */
function errorCodeOf(text: string): string {
  try {
    const value: unknown = JSON.parse(text);
    if (isRecord(value) && isRecord(value.error)) {
      const error = value.error;
      if (typeof error.code === 'string' && error.code !== '') return error.code.slice(0, 64);
      if (typeof error.type === 'string' && error.type !== '') return error.type.slice(0, 64);
    }
  } catch {
    // Not JSON: no code to read.
  }
  return '';
}

/** POST one request to the gateway's Chat Completions route, naming the model this entry dialled. */
async function dial(model: string, body: Json): Promise<Response> {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    authorization: `Bearer ${GATEWAY_KEY}`,
  };
  if (body.stream === true) headers.accept = 'text/event-stream';
  return fetch(chatCompletionsUrl(), {
    method: 'POST',
    headers,
    body: JSON.stringify(gatewayBody(body, model)),
  });
}

/** One gateway attempt's outcome: the response when it answered, or the refusal it cost. */
type Attempt =
  | { ok: true; response: Response }
  | { ok: false; failure: FailureRow; retrySame: boolean };

/** Dial one model once, classifying a refusal as worth one retry or as a move to the next model. */
async function attempt(model: string, body: Json): Promise<Attempt> {
  let upstream: Response;
  try {
    upstream = await dial(model, body);
  } catch (err) {
    // A broken connection is worth one retry on the same model, like a 5xx.
    return {
      ok: false,
      failure: { status: 502, code: 'upstream_unreachable', model },
      retrySame: true,
    };
  }
  if (upstream.ok) return { ok: true, response: upstream };
  const text = await upstream.text().catch(() => '');
  const failure: FailureRow = { status: upstream.status, code: errorCodeOf(text), model };
  if (upstream.status === 429) {
    // Rate-limited: the model is cooling down, and retrying it now would only cost another call.
    coolingDown.add(model);
    return { ok: false, failure, retrySame: false };
  }
  if (upstream.status >= 500) {
    // A 5xx may be transient: one retry on the same model, then the next-best.
    return { ok: false, failure, retrySame: true };
  }
  // Any other 4xx is a request this model will not take: move on, and cool it down.
  coolingDown.add(model);
  return { ok: false, failure, retrySame: false };
}

/**
 * Dial the prompt until one model answers it, or the attempts run out. The candidates are the
 * free models meeting the prompt's requirements, best-rated first, skipping the ones cooling
 * down. `toGateway` builds the Chat Completions body for the model the attempt dials. Returns
 * the answering model and its response, or nulls with the refusals the prompt cost.
 */
async function serve(
  toGateway: (modelId: string) => Json,
  requires: Requirement,
): Promise<{ model: ModelRow | null; response: Response | null; failures: FailureRow[] }> {
  const failures: FailureRow[] = [];
  const tried = new Set<string>();
  let attempts = 0;
  while (attempts < MAX_ATTEMPTS_PER_PROMPT) {
    const candidates = rankedCandidates(brief, requires, coolingDown).filter((m) => !tried.has(m.id));
    if (candidates.length === 0) return { model: null, response: null, failures };
    const model = candidates[0];
    attempts += 1;
    const result = await attempt(model.id, toGateway(model.id));
    if (result.ok) return { model, response: result.response, failures };
    failures.push(result.failure);
    if (result.retrySame && attempts < MAX_ATTEMPTS_PER_PROMPT) {
      // One retry on the same model, after a short pause; a second refusal cools it down.
      await pause(RETRY_PAUSE_MS);
      attempts += 1;
      const retry = await attempt(model.id, toGateway(model.id));
      if (retry.ok) return { model, response: retry.response, failures };
      failures.push(retry.failure);
      coolingDown.add(model.id);
    }
    tried.add(model.id);
  }
  return { model: null, response: null, failures };
}

/** A prompt no model would answer, as a 502 in the client's wire, with the refusals it cost. */
function givenUp(res: ServerResponse, wire: 'openai' | 'anthropic', failures: readonly FailureRow[]): void {
  const detail = failures.length > 0 ? ` after ${failures.length} refused call${failures.length === 1 ? '' : 's'}` : '';
  if (wire === 'anthropic') {
    sendError(res, wire, 502, 'api_error', `no free model answered this prompt${detail}`, failures);
  } else {
    sendError(res, wire, 502, 'upstream_error', `no free model answered this prompt${detail}`, failures);
  }
}

/** POST /v1/chat/completions: an OpenAI-shaped client, answered by the best free model that meets it. */
async function chat(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, 'openai', 400, 'invalid_request_error', 'the request body is not one JSON object');
  const requires = requirementsFor(body);
  const served = await serve(() => body, requires);
  if (served.response === null || served.model === null) return givenUp(res, 'openai', served.failures);
  const { model, response: upstream, failures } = served;
  if (upstream.body === null) return givenUp(res, 'openai', failures);
  if (body.stream === true) {
    const headers: Record<string, string> = {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    };
    if (failures.length > 0) headers['x-makefun-upstream-failures'] = trailHeader(failures);
    res.writeHead(200, headers);
    try {
      for await (const frame of openAIStreamWithModel(upstream.body, model.id)) res.write(frame);
    } finally {
      res.end();
    }
    return;
  }
  const answer: unknown = await upstream.json();
  sendJson(res, 200, openAIJsonWithModel(isRecord(answer) ? answer : {}, model.id), failures);
}

/** POST /v1/messages: an Anthropic-shaped client, rewritten to Chat Completions and answered back. */
async function messages(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, 'anthropic', 400, 'invalid_request_error', 'the request body is not one JSON object');
  const requires = requirementsFor(body);
  const served = await serve((modelId) => anthropicToOpenAI(body, modelId), requires);
  if (served.response === null || served.model === null) return givenUp(res, 'anthropic', served.failures);
  const { model, response: upstream, failures } = served;
  if (upstream.body === null) return givenUp(res, 'anthropic', failures);
  if (body.stream === true) {
    const headers: Record<string, string> = {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    };
    if (failures.length > 0) headers['x-makefun-upstream-failures'] = trailHeader(failures);
    res.writeHead(200, headers);
    try {
      for await (const event of anthropicEventsFromOpenAI(upstream.body, model.id)) res.write(event);
    } finally {
      res.end();
    }
    return;
  }
  const answer: unknown = await upstream.json();
  sendJson(res, 200, openAIToAnthropic(isRecord(answer) ? answer : {}, model.id), failures);
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    models(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
    await chat(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/messages') {
    await messages(req, res);
    return;
  }
  sendError(res, url.pathname === '/v1/messages' ? 'anthropic' : 'openai', 404, 'not_found', `no route ${req.method ?? ''} ${url.pathname}`);
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => resolve());
  });
}

/** POST /register with the port and the one catalog; the env answers with the trial's prompt count. */
async function register(port: number, report: CatalogReport): Promise<void> {
  let lastError: unknown = null;
  for (let i = 0; i < 3; i++) {
    try {
      const res = await fetch(`${ENV_URL}/register`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog: report }),
        signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
      const reply: unknown = await res.json();
      if (isRecord(reply) && typeof reply.requests === 'number') log(`registered: the trial holds ${reply.requests} prompts`);
      return;
    } catch (err) {
      lastError = err;
      await pause(RETRY_PAUSE_MS);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

/** Poll GET /work every 100 ms; exit 0 the moment it says done. */
function poll(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status !== 200) return;
        const work: unknown = await res.json();
        if (isRecord(work) && work.done === true) {
          clearInterval(timer);
          server.close();
          process.exit(0);
        }
      } catch {
        // The env is not reachable yet: try again on the next tick.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_INTERVAL_MS);
}

async function start(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  if (process.argv.includes('--list')) {
    // The same document the register body carries, byte for byte: print the one built object and
    // stop before any server starts. A process whose stdout is not flushed loses the tail, so wait
    // for it.
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  if (chatCompletionsUrl() === '') throw new Error('OPENAI_BASE_URL is not set');
  const server = createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) {
        sendError(res, req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai', 500, 'internal_error', 'the server failed to answer');
      }
    });
  });
  await listen(server);
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  const port = address.port;
  // 8080 is the env's on one layout; the OS's ephemeral range never holds it, and the guard says so.
  if (port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');
  log(`listening on 0.0.0.0:${port} with ${servedModels().length} free models`);
  await register(port, catalog);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  poll(server);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}
