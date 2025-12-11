/**
 * Supabase KV 数据库操作模块（替代 Deno.openKv）
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { Cookie, ApiKey, RequestLog, SystemSettings, ModelMapping } from "./types.ts";
import { Config } from "./config.ts";

// 读取 Supabase 设置
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_KEY = Deno.env.get("SUPABASE_ANON_KEY")!;

export const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

/**
 * ----------- KV 封装 -------------
 * 使用 kv_data (k: string, v: jsonb)
 */

// 生成 KV key
function makeKey(key: (string | number)[]): string {
  return key.join(":");
}

// KV Get
async function kvGet<T>(key: (string | number)[]): Promise<T | null> {
  const k = makeKey(key);
  const { data, error } = await supabase.from("kv_data").select("v").eq("k", k).single();

  if (error) return null;
  return data?.v ?? null;
}

// KV Set
async function kvSet(key: (string | number)[], value: any): Promise<void> {
  const k = makeKey(key);
  await supabase.from("kv_data").upsert({ k, v: value });
}

// KV Delete
async function kvDelete(key: (string | number)[]): Promise<void> {
  const k = makeKey(key);
  await supabase.from("kv_data").delete().eq("k", k);
}

// KV List (prefix 模拟)
async function kvList<T>(prefix: (string | number)[]): Promise<{ key: string; value: T }[]> {
  const p = makeKey(prefix);
  const { data, error } = await supabase
    .from("kv_data")
    .select("*")
    .like("k", `${p}%`);

  if (error || !data) return [];

  return data.map((row) => ({
    key: row.k,
    value: row.v as T,
  }));
}

/**
 * ------------- Cookie 操作 -------------
 */

export class CookieDB {
  private static PREFIX = ["cookies"];

  static async getAll(): Promise<Cookie[]> {
    const list = await kvList<Cookie>(this.PREFIX);
    const cookies = list.map(item => item.value);
    return cookies.sort((a, b) => a.createdAt - b.createdAt);
  }

  static async get(id: string): Promise<Cookie | null> {
    return await kvGet<Cookie>([...this.PREFIX, id]);
  }

  static async add(cookie: Omit<Cookie, "id" | "createdAt" | "updatedAt">): Promise<Cookie> {
    const newCookie: Cookie = {
      ...cookie,
      id: crypto.randomUUID(),
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    await kvSet([...this.PREFIX, newCookie.id], newCookie);
    return newCookie;
  }

  static async update(id: string, updates: Partial<Cookie>): Promise<boolean> {
    const existing = await this.get(id);
    if (!existing) return false;

    const updated: Cookie = {
      ...existing,
      ...updates,
      updatedAt: Date.now(),
    };

    await kvSet([...this.PREFIX, id], updated);
    return true;
  }

  static async delete(id: string): Promise<boolean> {
    const existing = await this.get(id);
    if (!existing || existing.isDefault) return false;

    await kvDelete([...this.PREFIX, id]);
    return true;
  }

  static async getValidCookies(): Promise<Cookie[]> {
    return (await this.getAll()).filter(c => c.isValid);
  }

  static async incrementFailCount(id: string): Promise<void> {
    const cookie = await this.get(id);
    if (!cookie) return;

    const newFailCount = cookie.failCount + 1;
    const isValid = newFailCount < Config.MAX_FAIL_NUM;

    await this.update(id, { failCount: newFailCount, isValid });
  }

  static async resetFailCount(id: string): Promise<void> {
    await this.update(id, { failCount: 0, isValid: true });
  }
}

/**
 * ------------- API Key 操作 -------------
 */

export class ApiKeyDB {
  private static PREFIX = ["apikeys"];

  static async getAll(): Promise<ApiKey[]> {
    const list = await kvList<ApiKey>(this.PREFIX);
    const keys = list.map(item => item.value);
    return keys.sort((a, b) => a.createdAt - b.createdAt);
  }

  static async get(id: string): Promise<ApiKey | null> {
    return await kvGet<ApiKey>([...this.PREFIX, id]);
  }

  static async getByKey(key: string): Promise<ApiKey | null> {
    const all = await this.getAll();
    return all.find(k => k.key === key) || null;
  }

  static async add(apiKey: Omit<ApiKey, "id" | "createdAt" | "requestCount">): Promise<ApiKey> {
    const newKey: ApiKey = {
      ...apiKey,
      id: crypto.randomUUID(),
      createdAt: Date.now(),
      requestCount: 0,
    };

    await kvSet([...this.PREFIX, newKey.id], newKey);
    return newKey;
  }

  static async update(id: string, updates: Partial<ApiKey>): Promise<boolean> {
    const existing = await this.get(id);
    if (!existing) return false;

    const updated = { ...existing, ...updates };
    await kvSet([...this.PREFIX, id], updated);
    return true;
  }

  static async delete(id: string): Promise<boolean> {
    const existing = await this.get(id);
    if (!existing || existing.isDefault) return false;

    await kvDelete([...this.PREFIX, id]);
    return true;
  }

  static async incrementRequestCount(key: string): Promise<void> {
    const apiKey = await this.getByKey(key);
    if (!apiKey) return;

    await this.update(apiKey.id, {
      requestCount: apiKey.requestCount + 1,
    });
  }

  static async getEnabledKeys(): Promise<ApiKey[]> {
    return (await this.getAll()).filter(k => k.isEnabled);
  }
}

/**
 * ------------- 请求日志 -------------
 */

export class RequestLogDB {
  private static PREFIX = ["logs"];

  static async add(log: Omit<RequestLog, "id">): Promise<void> {
    const newLog: RequestLog = {
      ...log,
      id: crypto.randomUUID(),
    };

    await kvSet([...this.PREFIX, newLog.timestamp, newLog.id], newLog);
    await this.cleanup();
  }

  static async getRecent(limit = Config.MAX_REQUEST_RECORD_NUM): Promise<RequestLog[]> {
    const list = await kvList<RequestLog>(this.PREFIX);
    const logs = list.map(item => item.value);

    return logs
      .sort((a, b) => b.timestamp - a.timestamp)
      .slice(0, limit);
  }

  private static async cleanup(): Promise<void> {
    const list = await kvList<RequestLog>(this.PREFIX);
    const logs = list.map(item => item.value);

    if (logs.length <= Config.MAX_REQUEST_RECORD_NUM) return;

    const toDelete = logs
      .sort((a, b) => b.timestamp - a.timestamp)
      .slice(Config.MAX_REQUEST_RECORD_NUM);

    for (const log of toDelete) {
      await kvDelete([...this.PREFIX, log.timestamp, log.id]);
    }
  }

  static async count(): Promise<number> {
    return (await kvList(this.PREFIX)).length;
  }

  static async countByPath(path: string): Promise<number> {
    const list = await kvList<RequestLog>(this.PREFIX);
    return list.filter(entry => entry.value.path === path).length;
  }
}

/**
 * ------------- 系统设置 -------------
 */

export class SystemSettingsDB {
  private static KEY = ["settings", "system"];

  static async get(): Promise<SystemSettings> {
    return (
      (await kvGet<SystemSettings>(this.KEY)) ||
      Config.getDefaultSystemSettings()
    );
  }

  static async update(settings: Partial<SystemSettings>): Promise<SystemSettings> {
    const current = await this.get();
    const updated = { ...current, ...settings };
    await kvSet(this.KEY, updated);
    return updated;
  }
}

/**
 * ------------- 模型映射 -------------
 */

export class ModelMappingDB {
  private static KEY = ["settings", "models"];

  static async get(): Promise<ModelMapping[]> {
    return (
      (await kvGet<ModelMapping[]>(this.KEY)) ||
      Config.DEFAULT_MODELS_MAPS
    );
  }

  static async update(mappings: ModelMapping[]): Promise<void> {
    await kvSet(this.KEY, mappings);
  }
}

/**
 * ------------- 初始化数据库 -------------
 */

export async function initializeDatabase(): Promise<void> {
  // Cookies
  const cookies = await CookieDB.getAll();
  if (cookies.length === 0 && Config.DEFAULT_CTO_COOKIES.length > 0) {
    for (const value of Config.DEFAULT_CTO_COOKIES) {
      await CookieDB.add({
        value,
        isValid: true,
        failCount: 0,
        isDefault: true,
      });
    }
  }

  // API Keys
  const keys = await ApiKeyDB.getAll();
  if (keys.length === 0) {
    for (const key of Config.DEFAULT_CHAT_APIKEYS) {
      await ApiKeyDB.add({
        key,
        isEnabled: true,
        isDefault: true,
      });
    }
  }

  console.log("✅ Supabase KV 模式：数据库初始化完成");
}
