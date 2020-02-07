/** @typedef { import('../types').EncodingValidationError } EncodingValidationError */

class ValidationError extends TypeError {
    /**
     * @param {Array<EncodingValidationError>} validationErrors 
     */
    constructor(validationErrors) {
        super('Failed to encode message with given schema')
        this.name = 'AvroSchemaValidationError'
        this.validationErrors = validationErrors
    }

    toJSON() {
        return {
          error: {
            name: this.name,
            message: this.message,
            stacktrace: this.stack,
            errors: this.validationErrors
          }
        }
    }
}

module.exports = {
    ValidationError
}
