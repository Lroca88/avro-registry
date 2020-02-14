const validationError = require("../../src/lib/validation-error");

describe("Validation Error Tests", () => {
  test("Validation error is instance of error", () => {
    const validationErrors = [
      {
        value: "Testing validation error"
      }
    ];

    const err = new validationError(validationErrors);
    expect(err).toBeInstanceOf(Error);
  });

  test("Return validation array as message", () => {
    const validationErrors = [
      {
        value: "Testing validation error"
      }
    ];
    errorsString = JSON.stringify(validationErrors, null, 2);

    const err = new validationError(validationErrors);
    expect(err.message.includes(errorsString)).toBe(true);
  });
});
