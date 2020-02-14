export interface EncodingValidationError {
  /**
   * Path of the field that failed validation
   */
  path: string;

  /**
   * Value of the field that failed to be decoded
   */
  value: any;

  /**
   * Type that the schema expects the value to be.
   */
  expectedType: string;
}

export interface SchemaRegistryConfig {
  /**
   * Authentication Key or UserName.
   */
  key?: string;

  /**
   * Authetication Secret or Password
   */
  secret?: string;

  /**
   * Set to true get better error messages when the message being sent doesn't match.
   * **Note that this has significant impact on performance and should only be enabled for development.**
   */
  validateMessages?: boolean;
}
