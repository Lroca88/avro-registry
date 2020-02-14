const http = require("http");
const nock = require("nock");
const decode = require("../../src/lib/decode");
const SchemaCache = require("../../src/lib/schema-cache");

describe("Decode Message Tests", () => {
  let schemaRegistry;
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

  test("rejects with error if there is no magic byte in the message buffer", async () => {
    const wrongMessageBuffer = Buffer.from("testing");
    try {
      await decode(schemaRegistry, wrongMessageBuffer);
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
      expect(e.message).toMatch(
        "Message needs the magic byte to be compliant with Confluent."
      );
    }
  });

  test("rejects with error when schema registry returns not found", async () => {
    nock("http://www.testing.com")
      .get("/schemas/ids/1")
      .reply(404, { error_code: 404, message: "Schema not found" });

    try {
      await decode(schemaRegistry, messageBuffer);
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
      expect(e.message).toMatch(
        "Schema registry error: 404 - Schema not found"
      );
    }
  });

  test("returns message decoded when the schema is fetched remotely", async () => {
    const schema = JSON.stringify({ type: "string" });
    const compareMessage = "testing";

    nock("http://www.testing.com")
      .get("/schemas/ids/1")
      .reply(200, { schema });

    const decoded = await decode(schemaRegistry, messageBuffer);
    expect(decoded).toMatch(compareMessage);
  });

  test("returns message decoded when schema is fetched from cache", async () => {
    const schema = JSON.stringify({ type: "string" });
    const compareMessage = "testing";

    nock("http://www.testing.com")
      .get("/schemas/ids/1")
      .reply(200, { schema });

    const decoded = await decode(schemaRegistry, messageBuffer);
    expect(decoded).toMatch(compareMessage);

    // Cleaning nock
    nock.cleanAll();
    nock.disableNetConnect();

    // Network dissabled, the schema needs to be in the cache
    const decoded2 = await decode(schemaRegistry, messageBuffer);
    expect(decoded2).toMatch(compareMessage);
  });
});
