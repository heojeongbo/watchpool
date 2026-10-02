export interface SseOptions {
	/** Defaults to the platform fetch; inject authentication or test transports. */
	fetch?: typeof globalThis.fetch;
	url: string | (() => string);
	headers?: HeadersInit;
	credentials?: RequestCredentials;
	/** UTF-16 character bound for a partial event, including field names. */
	maxEventChars?: number;
}
