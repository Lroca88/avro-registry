/** @typedef { import('../types').EncodingValidationError } EncodingValidationError */

class ValidationError extends TypeError {
  /**
   * @param {Array<EncodingValidationError>} validationErrors
   */
  constructor(validationErrors) {
    const validationMessage = JSON.stringify(validationErrors, null, 2);
    super(`Failed to encode message with given schema \n ${validationMessage}`);
    this.name = "AvroSchemaValidationError";
    this.validationErrors = validationErrors;
  }

  toJSON() {
    return {
      error: {
        name: this.name,
        message: this.message,
        stacktrace: this.stack,
        errors: this.validationErrors
      }
    };
  }
}

module.exports = ValidationError;
