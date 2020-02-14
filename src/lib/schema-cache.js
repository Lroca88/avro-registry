class SchemaCache {
  constructor() {
    this.schemaMapById = new Map();
    this.schemaMapBySchema = new Map();
  }

  setSchema(id, schema) {
    this.schemaMapById.set(id, schema);
    const strSchema = JSON.stringify(schema);
    this.schemaMapBySchema.set(strSchema, id);
    return id;
  }

  getSchemaById(id) {
    return this.schemaMapById.get(id);
  }

  getIdBySchema(schema) {
    const strSchema = JSON.stringify(schema);
    return this.schemaMapBySchema.get(strSchema);
  }
}

module.exports = SchemaCache;
