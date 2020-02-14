const http = require("http");
const nock = require("nock");
const {
  encodeMessageById,
  encodeMessageBySchema
} = require("../../src/lib/encode");
const SchemaCache = require("../../src/lib/schema-cache");

describe("Encode Message Tests", () => {
  let schemaRegistry;
  const id = 1;
  const schema = { type: "string" };
  const payload = "testing";
  const messageBuffer = Buffer.from([
    0,
    0,
    0,
    0,
    1,
    14,
    116,
    101,
    115,
    116,
    105,
    110,
    103
  ]);

  beforeEach(() => {
    schemaRegistry = {
      cache: new SchemaCache(),
      protocol: http,
      hostname: "www.testing.com",
      port: null,
      path: "/"
    };
  });

  afterEach(() => {
    nock.cleanAll();
  });

  describe("encode message by id", () => {
    test("returns an error if schema registry's request fails", async () => {
      nock("http://www.testing.com")
        .get(`/schemas/ids/${id}`)
        .reply(403, { error_code: 403, message: "Schema not found" });

      try {
        await encodeMessageById(schemaRegistry, id, payload);
      } catch (e) {
        expect(e).toBeInstanceOf(Error);
        expect(e.message).toMatch(
          "Schema registry error: 403 - Schema not found"
        );
      }
    });

    test("successfully encodes message when getting schema remotely", async () => {
      nock("http://www.testing.com")
        .get(`/schemas/ids/${id}`)
        .reply(200, { schema: JSON.stringify(schema) });
      const encoded = await encodeMessageById(schemaRegistry, id, payload);
      expect(encoded).toEqual(messageBuffer);
    });

    test("returns encoded message when schema is fetched from cache", async () => {
      nock("http://www.testing.com")
        .get(`/schemas/ids/${id}`)
        .reply(200, { schema: JSON.stringify(schema) });

      const encoded = await encodeMessageById(schemaRegistry, id, payload);
      expect(encoded).toEqual(messageBuffer);

      // Cleaning nock
      nock.cleanAll();
      nock.disableNetConnect();

      // Network dissabled, the schema needs to be in the cache
      const encoded2 = await encodeMessageById(schemaRegistry, id, payload);
      expect(encoded2).toEqual(messageBuffer);
    });
  });

  describe("encode message by schema", () => {
    test("returns an error if schema registry's fails responding", async () => {
      nock("http://www.testing.com")
        .post("/subjects/topic-value/versions")
        .reply(500, {
          error_code: 500,
          message: "Schema registry server error"
        });

      try {
        await encodeMessageBySchema(
          schemaRegistry,
          "value",
          "topic",
          schema,
          payload
        );
      } catch (e) {
        expect(e).toBeInstanceOf(Error);
        expect(e.message).toMatch(
          "Schema registry error: 500 - Schema registry server error"
        );
      }
    });

    test("uses topic-key correctly when encoding", async () => {
      nock("http://www.testing.com")
        .post("/subjects/topic-key/versions")
        .reply(200, { id });
      const encoded = await encodeMessageBySchema(
        schemaRegistry,
        "key",
        "topic",
        schema,
        payload
      );
      expect(encoded).toEqual(messageBuffer);
    });

    test("uses topic-value correctly when encoding", async () => {
      nock("http://www.testing.com")
        .post("/subjects/topic-value/versions")
        .reply(200, { id });
      const encoded = await encodeMessageBySchema(
        schemaRegistry,
        "value",
        "topic",
        schema,
        payload
      );
      expect(encoded).toEqual(messageBuffer);
    });

    test("returns encoded message when schema is already in cache", async () => {
      nock("http://www.testing.com")
        .post("/subjects/topic-value/versions")
        .reply(200, { id });

      const encoded = await encodeMessageBySchema(
        schemaRegistry,
        "value",
        "topic",
        schema,
        payload
      );
      expect(encoded).toEqual(messageBuffer);

      // Cleaning nock
      nock.cleanAll();
      nock.disableNetConnect();

      // Network dissabled, the schema needs to be in the cache
      const encoded2 = await encodeMessageBySchema(
        schemaRegistry,
        "value",
        "topic",
        schema,
        payload
      );
      expect(encoded2).toEqual(messageBuffer);
    });
  });
});
