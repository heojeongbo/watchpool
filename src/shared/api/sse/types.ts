export interface SseOptions {
	url: string | (() => string);
	headers?: HeadersInit;
	credentials?: RequestCredentials;
	/** UTF-16 character bound for a partial event, including field names. */
	maxEventChars?: number;
}
