
export class ApiError extends Error {
    constructor(message: string, public response?: any) {
      super(message);
      this.name = "ApiError";
    }
  }
  
 export class AuthorizationError extends ApiError {
    constructor(message: string, public response?: any) {
      super(message, response);
      this.name = "AuthorizationError";
    }
  }