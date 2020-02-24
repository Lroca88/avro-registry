const nock = require("nock");
const schemaRegistry = require("../index");

describe("Schema Registry Tests", () => {
  test("returns an object with five functions", () => {
    const reg = schemaRegistry("http://www.testing.com");
    expect(typeof reg === "object").toBe(true);
    expect(reg.decodeMessage).toBeInstanceOf(Function);
    expect(reg.encodeMessage).toBeInstanceOf(Function);
    expect(reg.encodeById).toBeInstanceOf(Function);
    expect(reg.encodedKey).toBeInstanceOf(Function);
    expect(reg.getTypes).toBeInstanceOf(Function);
  });

  test("chooses the protocol https correctly", async () => {
    const topic = "testing";
    const schema = { type: "int" };
    const payload = 123456;
    const reg = schemaRegistry("https://www.testing.com");
    nock("https://www.testing.com")
      .post(`/subjects/${topic}-value/versions`)
      .reply(200, { id: 10001 });

    return await reg.encodeMessage(topic, schema, payload);
  });

  test("chooses the protocol http correctly", async () => {
    const topic = "testing";
    const schema = { type: "string" };
    const payload = "Any here";
    const reg = schemaRegistry("http://www.testing.com");
    nock("http://www.testing.com")
      .post(`/subjects/${topic}-value/versions`)
      .reply(200, { id: 10001 });

    return await reg.encodeMessage(topic, schema, payload);
  });

  test("implements basic authentication", async () => {
    const topic = "testing";
    const schema = { type: "string" };
    const payload = "Any here";
    const reg = schemaRegistry("https://www.testing.com", {
      key: "John",
      secret: "Smith"
    });
    nock("https://www.testing.com")
      .post(`/subjects/${topic}-value/versions`)
      .basicAuth({ user: "John", pass: "Smith" })
      .reply(200, { id: 10001 });

    return await reg.encodeMessage(topic, schema, payload);
  });

  test("handles connection errors", async () => {
    const topic = "testing";
    const schema = { type: "string" };
    const payload = "Any here";
    const reg = schemaRegistry("https://bad-resource");
    try {
      await reg.encodeMessage(topic, schema, payload);
    } catch (e) {
      expect(e.code).toMatch("ENOTFOUND");
      expect(e.message).toContain("getaddrinfo ENOTFOUND bad-resource");
    }
  });

  test("message validation throws before encoding", async () => {
    const topic = "testing";
    const schema = { type: "string" };
    const payload = 1234567890;
    const validateMessages = true;
    const reg = schemaRegistry("https://www.testing.com", { validateMessages });
    nock("https://www.testing.com")
      .post(`/subjects/${topic}-value/versions`)
      .reply(200, { id: 10001 });

    try {
      await reg.encodeMessage(topic, schema, payload);
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
      expect(e.message.includes(`"value": ${payload}`)).toBe(true);
      expect(e.message.includes('"expectedType": "\\"string\\""')).toBe(true);
    }
  });

  test("message validation pass without issues before encoding", async () => {
    const topic = "testing";
    const schema = { type: "string" };
    const payload = "testing ok";
    const validateMessages = true;
    const reg = schemaRegistry("https://www.testing.com", { validateMessages });
    nock("https://www.testing.com")
      .post(`/subjects/${topic}-value/versions`)
      .reply(200, { id: 10001 });

    return await reg.encodeMessage(topic, schema, payload);
  });
});
