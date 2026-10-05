import type { OpenOptions, Parameters } from './types.mjs';
export type Request = { id: number } & (
  { action: 'open'; options: OpenOptions } |
  { action: 'query'; sql: string; parameters: Parameters; maxRows: number; firstRow: boolean } |
  { action: 'exec'; sql: string; parameters: Parameters } |
  { action: 'prepare'; sql: string } |
  { action: 'statement'; statement: number; parameters: Parameters; maxRows: number; mode: 'all' | 'first' | 'discard' } |
  { action: 'finalize'; statement: number } |
  { action: 'stats' | 'clearCache' | 'close' }
);
export interface Reply { id: number; result?: unknown; error?: string }
