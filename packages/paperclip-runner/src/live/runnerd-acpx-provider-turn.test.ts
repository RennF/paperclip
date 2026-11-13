import { createHash } from "node:crypto";
import { execFileSync, spawn } from "node:child_process";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { closeSync, openSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, it, vi } from "vitest";
import type { DurablePrpControlPlane } from "../control-plane/durable-prp-control-plane.js";
import { QUALIFIED_ACPX_PROFILES, resolveQualifiedAcpxProfile } from "../drivers/acpx/qualified-profiles.js";
import { createCapabilityRunnerdCodexTransport, readRunnerdArtifactBinding } from "./runnerd-codex-transport.js";
import { createRunnerProviderLaunch, runnerLaunchRequirementsDigest } from "./runner-provider-launch.js";
import { defaultCapabilityRunnerdBinary } from "../../test/support/runnerd-transport.js";
import type { PrpEvent } from "../protocol/replay-contract.js";
import { createNativeSessionBackend } from "../backends/native-backend-factory.js";
import type { NativeExecutionInput } from "../contracts/native-execution.js";
import { executeNativeSession } from "../native-session-runtime.js";

const sha = (value: string | Buffer) => `sha256:${createHash("sha256").update(value).digest("hex")}`;
const quote = (value: string) => `'${value.replaceAll("'", "'\"'\"'")}'`;
const kill = (pid: number) => { try { process.kill(-pid, "SIGKILL"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; } };
const alive = (pid: number) => {
  try { process.kill(pid, 0); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; throw error; }
  try { return !execFileSync("ps", ["-p", String(pid), "-o", "stat="], { encoding: "utf8" }).trim().startsWith("Z"); }
  catch (error) { if ((error as { status?: number }).status === 1) return false; throw error; }
};

function qualifiedProfile(agent: "claude" | "codex") {
  const qualified = resolveQualifiedAcpxProfile(agent, "explicit-source-model");
  return { driverKind: qualified.driverKind, protocolVersion: qualified.protocolVersion, acpxVersion: qualified.acpxVersion,
    agent: qualified.agent, agentProfileVersion: qualified.agentProfileVersion,
    agentServerPackage: qualified.agentServerPackage, agentServerVersion: qualified.agentServerVersion,
    agentRuntimePackage: qualified.agentRuntimePackage, agentRuntimeVersion: qualified.agentRuntimeVersion,
    commandDigest: qualified.commandDigest };
}

let scenario = 0;

async function runProviderTurnScenario(tokens: string, options: { agent?: "claude" | "codex"; capMs?: number } = {}) {
  const agent = options.agent ?? "claude";
  const n = ++scenario;
  const root = await realpath(await mkdtemp(join(tmpdir(), "runner-acpx-provider-turn-")));
  const journal = join(root, "calls"), fault = join(root, "fault"), wrapper = join(root, "sidecar"), script = join(root, "sidecar-entry");
  const background = join(root, "background");
  const binary = defaultCapabilityRunnerdBinary(), sidecar = resolve(import.meta.dirname, "../../runner/target/debug/fake-acpx-sidecar");
  await mkdir(join(root, "acpx"), { mode: 0o700 }); await mkdir(join(root, "diagnostics"), { mode: 0o700 });
  await writeFile(script, "qualified fixture entry");
  await writeFile(background, tokens);
  await writeFile(wrapper, `#!/bin/sh\nexec ${quote(sidecar)} --mode source-reconcile --profile-digest ${quote(QUALIFIED_ACPX_PROFILES[agent].commandDigest)} --journal ${quote(journal)} --source-fault ${quote(fault)} --background-tasks ${quote(background)} --spawn-owned-child\n`, { mode: 0o700 });
  let authority: DurablePrpControlPlane | null = null;
  const server = createServer();
  server.on("upgrade", (request, socket, head) => authority ? authority.handleUpgrade(request, socket, "/runner", head) : socket.destroy());
  await new Promise<void>(done => server.listen(0, "127.0.0.1", done));
  const address = server.address(); if (!address || typeof address === "string") throw new Error("missing fixture listener");
  const identity = { runnerInstanceId: `runner-provider-turn-${n}`, environmentLeaseId: `lease-provider-turn-${n}`, runId: `run-provider-turn-${n}`,
    normalizedSessionId: `session-provider-turn-${n}`, turnId: `turn-provider-turn-${n}`, itemId: `item-provider-turn-${n}` };
  const nativeIdentity = { companyId: `company-${n}`, agentId: "agent", issueId: "issue", runId: identity.runId, sessionId: identity.normalizedSessionId };
  const execution: NativeExecutionInput = { schema: "paperclip.native-execution-input.v1",
    binding: { companyId: nativeIdentity.companyId, agentId: nativeIdentity.agentId, issueId: nativeIdentity.issueId, runId: identity.runId, executionWorkspaceId: "workspace" },
    task: { identifier: "TASK-1", title: "Install and test", description: null, prompt: "Install and test", workMode: "standard" },
    workspace: { cwd: root, repoUrl: null, repoRef: null, branchName: null },
    session: { normalizedSessionId: identity.normalizedSessionId, driverKind: "acpx_runtime", protocolVersion: 1, lifecyclePolicy: { mode: "per_turn", idleTimeoutMs: null } },
    provider: { kind: "acpx", agent, model: "explicit-source-model", permissionPolicy: "interactive", profile: qualifiedProfile(agent) },
    completionContract: { id: "contract", sha256: sha("contract"), schemaVersion: "paperclip.completion-contract.v1",
      contract: { revision: "acpx-provider-turns-v1", objective: "Install and test", criteria: [{ id: "objective", requirement: "Install and test" }] } },
    interactionResponses: [], credentialBindings: [] };
  const runnerPids = new Set<number>();
  const bundle = createCapabilityRunnerdCodexTransport({
    provider: "acpx", acpxAgent: agent, runnerBinary: binary, stateDirectory: root, prpIdentity: identity,
    runnerArtifact: readRunnerdArtifactBinding(binary), codexSourceRecoveryVersion: 1, acpxSourceRecoveryVersion: 1,
    lifecyclePolicy: { mode: "per_turn", idleTimeoutMs: null }, runnerProcessReconnectGraceMs: null, runnerReconnectGraceMs: 3_000,
    controlPlaneRegistration: async next => { authority = next; return { connectUrl: `ws://127.0.0.1:${address.port}/runner`, release: () => { authority = null; } }; },
    prepareLaunch: async launch => ({ configurationId: `initial-${launch.identity.runId}`, requirementsDigest: runnerLaunchRequirementsDigest(launch),
      provider: createRunnerProviderLaunch(launch, { options: {}, root, cwd: root, codexHome: root, codexCommand: "", providerNodeCommand: wrapper, opencodeProxyPath: "", acpxSidecarPath: script }) }),
    runnerProcessLauncher: spec => {
      const stderr = openSync(join(root, "native.stderr"), "a", 0o600);
      const child = spawn(binary, [...spec.args, "--acpx-launch-authority-digest", sha("fixture authority"),
        "--acpx-sidecar-command", wrapper, "--acpx-sidecar-command-sha256", sha(execFileSync("cat", [wrapper])),
        "--acpx-sidecar-script", script, "--acpx-sidecar-script-sha256", sha("qualified fixture entry")],
        { env: { PATH: process.env.PATH, HOME: root, ...spec.environment }, stdio: ["ignore", "ignore", stderr], detached: true });
      closeSync(stderr);
      if (child.pid) runnerPids.add(child.pid);
      return { child, completion: new Promise((done, reject) => { child.once("error", reject); child.once("exit", (code, signal) => done({ code, signal, stdout: "", stderr: "" })); }) };
    },
  });
  const calls = async () => (await readFile(journal, "utf8").catch(() => "")).trim().split("\n").filter(Boolean)
    .flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
  const appended: PrpEvent[] = [];
  const completeRun = vi.fn(async () => undefined);
  const resolveMissingResult = vi.fn(async () => null);
  try {
    const backend = createNativeSessionBackend(execution, { codexTransportFactory: () => bundle.transport,
      environment: { PAPERCLIP_WORKSPACE_CWD: root }, dynamicTools: [] });
    const started = Date.now();
    const outcome = await executeNativeSession({
      input: execution, backend, runnerInstanceId: identity.runnerInstanceId, controlPlaneInstanceId: "control-provider-turn",
      controlPlane: {
        async openRun() {}, async checkpointSession() {},
        async appendEvent(value: PrpEvent) {
          appended.push(structuredClone(value));
          return { cursor: value.sourceSeq, highestContiguousSourceSeq: value.sourceSeq, disposition: "committed" };
        },
        async replayEvents() { return { events: [], highestContiguousSourceSeq: 0 }; },
        completeRun,
      },
      timeoutMs: 30_000, resolveMissingResult,
      providerTurnWait: options.capMs === undefined ? undefined : { capMs: options.capMs },
    }).then(() => "finalized", (error: unknown) => String(error instanceof Error ? error.message : error));
    const journalled = await calls();
    return { outcome, appended, completeRun, resolveMissingResult, journalled, root, elapsedMs: Date.now() - started };
  } catch (error) {
    const diagnostics = (await readFile(join(root, "native.stderr"), "utf8").catch(() => ""))
      + (await readFile(join(root, "diagnostics", "runnerd.stderr.log"), "utf8").catch(() => ""));
    throw new Error(`${String(error)}\n${diagnostics}`, { cause: error });
  } finally {
    await bundle.transport.close().catch(() => undefined);
    for (const pid of runnerPids) kill(pid);
    for (const entry of await calls()) if (entry.processStart) kill(entry.processStart);
    server.closeAllConnections(); await new Promise<void>(done => server.close(() => done()));
    await rm(root, { recursive: true, force: true });
  }
}

const sidecarCommands = (journalled: Array<Record<string, unknown>>) => journalled.filter(entry => typeof entry.command === "string").map(entry => entry.command);

it.skipIf(process.platform === "win32")("continues a run in the provider's own turn after background work and settles its result", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-result");
  expect(run.outcome).toBe("finalized");
  expect(run.completeRun).toHaveBeenCalledOnce();
  expect(run.resolveMissingResult).not.toHaveBeenCalled();
  const awaited = run.appended.find(event => event.eventType === "turn.completed" && event.payload.awaitingProviderTurn === true);
  expect(awaited).toBeDefined();
  const started = run.appended.filter(event => event.eventType === "turn.started");
  expect(started).toHaveLength(2);
  expect(started[1]).toMatchObject({ turnId: "provider-1", payload: { origin: "provider" } });
  expect(run.appended.filter(event => event.payload.kind === "usage").map(event => event.turnId)).toContain("provider-1");
  expect(sidecarCommands(run.journalled)).toContain("session.provider_turn");
}, 60_000);

it.skipIf(process.platform === "win32")("ends the wait at the cap and takes today's missing-result path when the provider never starts its turn", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-turn-hold", { capMs: 300 });
  expect(run.outcome).toContain("native_finalization_missing");
  expect(run.completeRun).not.toHaveBeenCalled();
  expect(run.resolveMissingResult).toHaveBeenCalledWith(expect.objectContaining({
    terminalEvent: expect.objectContaining({ eventType: "turn.completed", payload: expect.objectContaining({ awaitingProviderTurn: true }) }) }));
  expect(run.appended.some(event => event.payload.status === "provider_turn_wait_ended")).toBe(true);
  expect(run.appended.some(event => event.payload.origin === "provider")).toBe(false);
  const processes = run.journalled.filter(entry => typeof entry.processStart === "number").map(entry => entry.processStart as number);
  expect(processes.length).toBeGreaterThan(0);
  await vi.waitFor(() => { for (const pid of processes) expect(alive(pid)).toBe(false); }, { timeout: 10_000 });
}, 60_000);

it.skipIf(process.platform === "win32")("ends the wait in seconds, not at the cap, when the provider process is lost while awaiting", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-turn-hold provider-lost-after-arm", { capMs: 30_000 });
  expect(run.outcome).toContain("native_finalization_missing");
  expect(run.completeRun).not.toHaveBeenCalled();
  expect(run.appended.some(event => event.payload.awaitingProviderTurn === true)).toBe(true);
  expect(run.elapsedMs).toBeLessThan(15_000);
}, 60_000);

it.skipIf(process.platform === "win32")("ends the wait in seconds, not at the cap, when the sidecar exits while awaiting", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-turn-hold provider-sidecar-exit-after-arm", { capMs: 30_000 });
  expect(run.completeRun).not.toHaveBeenCalled();
  expect(run.appended.some(event => event.payload.awaitingProviderTurn === true)).toBe(true);
  expect(run.elapsedMs).toBeLessThan(15_000);
}, 60_000);

it.skipIf(process.platform === "win32")("leaves a Claude turn without background work exactly as before", async () => {
  const run = await runProviderTurnScenario("quiet provider-turn");
  expect(run.outcome).toContain("native_finalization_missing");
  expect(run.appended.some(event => event.payload.awaitingProviderTurn === true)).toBe(false);
  expect(run.appended.some(event => event.payload.status === "provider_turn_wait_ended")).toBe(false);
  expect(sidecarCommands(run.journalled)).not.toContain("session.provider_turn");
}, 60_000);

it.skipIf(process.platform === "win32")("never opts a non-Claude ACPX agent in", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-result", { agent: "codex" });
  expect(run.outcome).toContain("native_finalization_missing");
  expect(run.appended.some(event => event.payload.awaitingProviderTurn === true)).toBe(false);
  expect(sidecarCommands(run.journalled)).not.toContain("session.provider_turn");
}, 60_000);
