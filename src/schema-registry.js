/** @typedef { import('./types').SchemaRegistryConfig } SchemaRegistryConfig */

const http = require("http");
const https = require("https");

const SchemaCache = require("./lib/schema-cache");
const decode = require("./lib/decode");
const { encodeMessageById, encodeMessageBySchema } = require("./lib/encode");

/**
 * Schema Registry
 * @param {string} schemaRegistryUrl Url for schema registry rest api
 * @param {SchemaRegistryConfig} config Can handle auth and error information
 */
function schemaRegistry(schemaRegistryUrl, config = null) {
  const urlSections = new URL(schemaRegistryUrl);
  const schemaRegistry = {
    protocol: urlSections.protocol.includes("https") ? https : http,
    hostname: urlSections.hostname,
    port: urlSections.port,
    path: urlSections.path != null ? urlSections.path : "/",
    cache: new SchemaCache()
  };
  // Config, authentication and detailed error desc
  if (config && typeof config === "object") {
    if (config.key && config.secret) {
      schemaRegistry.key = config.key;
      schemaRegistry.secret = config.secret;
    }
    if (config.validateMessages) {
      schemaRegistry.validateMessages = true;
    }
  }

  const encodeMessage = (topic, schema, message, options) =>
    encodeMessageBySchema(
      schemaRegistry,
      "value",
      topic,
      schema,
      message,
      options
    );
  const encodedKey = (topic, schema, message, options) =>
    encodeMessageBySchema(
      schemaRegistry,
      "key",
      topic,
      schema,
      message,
      options
    );
  const encodeById = (schemaId, message, options) =>
    encodeMessageById(schemaRegistry, schemaId, message, options);
  const decodeMessage = (message, options) =>
    decode(schemaRegistry, message, options);

  return {
    encodeMessage,
    encodeById,
    encodedKey,
    decodeMessage
  };
}

module.exports = schemaRegistry;
