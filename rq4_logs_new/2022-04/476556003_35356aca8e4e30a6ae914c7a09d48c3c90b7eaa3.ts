import * as redis from "redis";
import config from "../config";
export default class RedisHelper {
    private static client: redis.RedisClientType;
    static async connection() {
        const { password, host, port } = config.redis;
        RedisHelper.client = redis.createClient({
            url: `redis://:${password}@${host}:${port}`
        });
        await RedisHelper.client.connect();
        RedisHelper.client.on("error", (error) => {
            console.log("Redis connection error!");
            console.log(error);
            RedisHelper.client.disconnect();
        });
        RedisHelper.client.on("ready", () => {
            console.log("Redis connected!");
        });
    }

    public static async setAsync(key: string, value: string | object, expire: number = 24 * 60 * 60) {
        let _value = value;

        if (!_value) return;

        if (typeof value === "object") {
            _value = JSON.stringify(value);
        }

        if (expire) {
            await RedisHelper.client.setEx(key, expire, `${_value}`);
        }
        else {
            await RedisHelper.client.set(key, `${_value}`);
        }
    }

    public static async getAsync(key: string): Promise<string> {
        const value = await RedisHelper.client.get(key);
        return value;
    }

    public static async getByAsync<T>(key: string): Promise<T> {
        const value = await RedisHelper.client.get(key);
        if (value) {
            return <T>JSON.parse(value);
        }
        return null;
    }
    public static async delAsync(key: string) {
        return await RedisHelper.client.del(key);
    }

    public static async generateKeyAsync(key: object | string) {
        if (typeof key === "object") {
            key = JSON.stringify(key);
        }

        return Buffer.from(key).toString("base64");
    }
}