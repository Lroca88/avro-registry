const http = require("http");
const nock = require("nock");
const pushSchema = require("../../src/lib/schema-push");
const SchemaCache = require("../../src/lib/schema-cache");

describe("Schema Push Tests", () => {
  let schemaRegistry;
  const schema = { type: "string" };

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

  test("rejects promise if push request fails", async () => {
    nock("http://www.testing.com")
      .post("/subjects/topic-value/versions")
      .reply(402, { error_code: 402, message: "Avro Schema is invalid" });

    try {
      await pushSchema(schemaRegistry, "topic", schema);
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
      expect(e.message).toMatch(
        "Schema registry error: 402 - Avro Schema is invalid"
      );
    }
  });

  test("returns schema id if the push request is successful", async () => {
    const id = 1001;
    nock("http://www.testing.com")
      .post("/subjects/topic-value/versions")
      .reply(200, { id });

    const res = await pushSchema(schemaRegistry, "topic", schema);
    expect(res).toEqual(id);
  });

  test("uses topic-key correctly when pushing a key schema", async () => {
    const id = 1002;
    nock("http://www.testing.com")
      .post("/subjects/topic-key/versions")
      .reply(200, { id });

    const res = await pushSchema(schemaRegistry, "topic", schema, "key");
    expect(res).toEqual(id);
  });

  test("uses topic-value correctly when pushing a value schema", async () => {
    const id = 1003;
    nock("http://www.testing.com")
      .post("/subjects/topic-value/versions")
      .reply(200, { id });

    const res = await pushSchema(schemaRegistry, "topic", schema, "value");
    expect(res).toEqual(id);
  });
});
