import { useCallback, useEffect, useRef, useSyncExternalStore } from "react";
import {
	IDLE_STATE,
	type Observer,
	type StreamAdapter,
	type WatchOptions,
	type WatchPool,
	type WatchState,
} from "../../../shared/lib/watchpool/index.js";

export interface UseWatchOptions<T>
	extends Omit<Observer<T>, "notify">,
		WatchOptions {
	enabled?: boolean;
}

/** Callbacks and source factories may change identity without reopening an equal key. */
export function useWatch<T>(
	pool: WatchPool<T>,
	key: string,
	source: StreamAdapter<T>,
	options: UseWatchOptions<T> = {},
): WatchState {
	const { enabled = true, staleAfterMs } = options;
	const current = useRef({ source, options });
	useEffect(() => {
		current.current = { source, options };
	});
	const subscribe = useCallback(
		(notify: () => void) => {
			if (!enabled) return () => {};
			return pool.subscribe(
				key,
				current.current.source,
				{
					notify,
					onMessage: (value) => current.current.options.onMessage?.(value),
					onOpen: () => current.current.options.onOpen?.(),
					onError: (error) => current.current.options.onError?.(error),
				},
				{ staleAfterMs },
			);
		},
		[pool, key, enabled, staleAfterMs],
	);
	const snapshot = useCallback(
		() => (enabled ? pool.getSnapshot(key) : IDLE_STATE),
		[pool, key, enabled],
	);
	return useSyncExternalStore(subscribe, snapshot, () => IDLE_STATE);
}
