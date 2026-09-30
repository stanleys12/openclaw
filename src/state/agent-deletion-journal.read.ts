import path from "node:path";
import { executeSqliteQuerySync, getNodeSqliteKysely } from "../infra/kysely-sync.js";
import {
  isSqliteCorruptionError,
  runSqliteDeferredTransactionSync,
} from "../infra/sqlite-transaction.js";
import { withExistingOpenClawStateDatabaseReadOnly } from "./openclaw-state-db-readonly.js";
import { tableExists } from "./openclaw-state-db-schema-helpers.js";
import type { DB as OpenClawStateKyselyDatabase } from "./openclaw-state-db.generated.js";

type RetainedAgentDeletion = {
  agentId: string;
  databasePaths: readonly string[];
};

export type RetainedAgentDeletionDisposition = readonly RetainedAgentDeletion[] | "unavailable";

function parseDatabasePaths(value: string): string[] {
  const parsed: unknown = JSON.parse(value);
  if (Array.isArray(parsed) && parsed.every((entry) => typeof entry === "string")) {
    return parsed;
  }
  throw new Error("Invalid agent deletion database path journal.");
}

/** Read completed retained deletions without initializing or repairing journal history. */
export function readRetainedAgentDeletions(
  env: NodeJS.ProcessEnv,
): RetainedAgentDeletionDisposition {
  try {
    return (
      withExistingOpenClawStateDatabaseReadOnly(
        ({ db }) => {
          if (!tableExists(db, "agent_deletion_journal")) {
            return "unavailable" as const;
          }
          const database =
            getNodeSqliteKysely<Pick<OpenClawStateKyselyDatabase, "agent_deletion_journal">>(db);
          return runSqliteDeferredTransactionSync(db, () =>
            executeSqliteQuerySync(
              db,
              database
                .selectFrom("agent_deletion_journal")
                .select(["agent_id", "agent_dir", "database_paths_json"])
                .where("cleanup_completed", "=", 1)
                .where("delete_files", "=", 0)
                .orderBy("agent_id", "asc"),
            ).rows.map((row) => ({
              agentId: row.agent_id,
              databasePaths: [
                path.join(row.agent_dir, "openclaw-agent.sqlite"),
                ...parseDatabasePaths(row.database_paths_json),
              ],
            })),
          );
        },
        { env },
      ) ?? "unavailable"
    );
  } catch (error) {
    if (isSqliteCorruptionError(error)) {
      throw error;
    }
    return "unavailable";
  }
}
