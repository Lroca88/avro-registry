const nock = require("nock");
const SchemaCache = require("../../src/lib/schema-cache");

describe("Schema Cache Tests", () => {
  let cache;
  let schema = { type: "int" };

  beforeEach(() => {
    cache = new SchemaCache();
  });

  test("should update schemaMapById when inserting schema and id", () => {
    const id = 1;
    cache.setSchema(id, schema);
    expect(cache.schemaMapById.get(id)).toEqual(schema);
  });

  test("should update schemaMapBySchema when inserting schema and id", () => {
    const id = 1;
    const schemaString = JSON.stringify(schema);
    cache.setSchema(id, schema);
    expect(cache.schemaMapBySchema.get(schemaString)).toEqual(id);
  });

  test("should return schema id when inserting new schema", () => {
    const id = 1;
    expect(cache.setSchema(id, schema)).toEqual(id);
  });

  test("returns schema when calling getSchemaById", () => {
    const id = 1;
    cache.setSchema(id, schema);
    expect(cache.getSchemaById(id)).toEqual(schema);
  });

  test("returns 'undefined' when getSchemaById can't find the id", () => {
    expect(cache.schemaMapById).toEqual(new Map());
    expect(cache.getSchemaById(1)).toBeUndefined();
  });

  test("returns id when calling getIdBySchema", () => {
    const id = 1;
    cache.setSchema(id, schema);
    expect(cache.getIdBySchema(schema)).toEqual(id);
  });

  test("returns 'undefined' when schemaMapBySchema can't find the schema", () => {
    expect(cache.schemaMapBySchema).toEqual(new Map());
    expect(cache.getIdBySchema(schema)).toBeUndefined();
  });
});
