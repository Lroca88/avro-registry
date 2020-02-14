const http = require("http");
const nock = require("nock");
const fetchSchema = require("../../src/lib/schema-fetch");
const SchemaCache = require("../../src/lib/schema-cache");

describe("Schema Fetch Tests", () => {
  let schemaRegistry;

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

  test("rejects promise if the request fails", async () => {
    const id = 1;
    nock("http://www.testing.com")
      .get(`/schemas/ids/${id}`)
      .reply(404, { error_code: 404, message: "Schema not found" });

    try {
      await fetchSchema(schemaRegistry, id);
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
      expect(e.message).toMatch(
        "Schema registry error: 404 - Schema not found"
      );
    }
  });

  test("resolves the schema if the request is successful", async () => {
    const id = 1;
    const schema = { type: "string" };
    nock("http://www.testing.com")
      .get("/schemas/ids/1")
      .reply(200, { schema: JSON.stringify(schema) });

    const res = await fetchSchema(schemaRegistry, id);
    expect(res).toEqual(schema);
  });
});
