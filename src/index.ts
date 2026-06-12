const DB_NAME = "webproxy-client-db";
const DB_VERSION = 1;
const STORE_NAME = 'CacheStore'

const CACHE_DURATION = 5184000000; // 60 days
const DEFAULT_WAITING_TIME = 400;

let ws: WebSocket;
let reconnectionDelay = 1_000;

const pendingRequests = new Map<string, string | null>();


async function cleanup(): Promise<void> {
	const cutoffTime = Date.now() - CACHE_DURATION 

	const db = await openDB()
	const tx = db.transaction(STORE_NAME, 'readwrite');
	const store = tx.objectStore(STORE_NAME);

	const cursorRequest = store.openCursor();
	cursorRequest.onsuccess = (e: any) => {
		const cursor = e?.target?.result;
		if (cursor && cursor.value.createdAt < cutoffTime) {
			cursor.delete();
		} 
	}
}

export function start(baseServerUrl: string, authorizationToken: string) {
	cleanup();

	let rebooted = false;
	let intervalId: number;

	return new Promise((resolve, reject) => {
		if (!baseServerUrl.startsWith('https://') && !baseServerUrl.startsWith('http://localhost:')) {
			return reject(new Error('Please provide a TLS-protected URL or localhost'));
		}
		ws = new WebSocket(`${baseServerUrl}/ws`)

		ws.addEventListener('error', (error) => {
			console.error('[WebProxy] WebSocket connection error:', error)
			if (!rebooted) {
				rebooted = true;
				clearInterval(intervalId)
				ws.close();
				setTimeout(() => start(baseServerUrl, authorizationToken), Math.ceil(Math.random() * 30_000) + reconnectionDelay);
				reconnectionDelay *= 2;
			}
		})	

		ws.addEventListener('close', (event) => {
			console.error('[WebProxy] WebSocket connection closed unexpectedly:', event);
			if (!rebooted) {
				rebooted = true;
				clearInterval(intervalId)
				setTimeout(() => start(baseServerUrl, authorizationToken), Math.ceil(Math.random() * 30_000) + reconnectionDelay);
				reconnectionDelay *= 2;
			}
		})

		ws.addEventListener('message', message => {
			// Successfully subscribed
			if (message.data === 'subscribed' && ws.readyState === WebSocket.OPEN) {
				return resolve(true);
			}

			// Provide
			if (typeof message.data === 'string' && message.data.startsWith('/p ')) {
				const [_command, name, ...rest] = message.data.split(' ')
				if (name && rest.length && pendingRequests.has(name) && pendingRequests.get(name) === null) {
					pendingRequests.set(name, rest.join(' '))
				}
			}
			
			// Request
			if (typeof message.data === 'string' && message.data.startsWith('/r ')) {
				provideRequestedData(message.data.replace('/r ', ''))
			}
		});

		ws.addEventListener('open', () => {
			intervalId = setInterval(() => {
				if (ws.readyState === WebSocket.OPEN) {
					ws.send('ping')
				}
			}, 30_000);

			ws.send(`/s ${authorizationToken}`);
		})

	});
}

async function provideRequestedData(json: string) {
	const obj = JSON.parse(json);
	const response = await readDatagram({ keepResourceFor: 0, resourceName: obj.resourceName, resourceScopes: obj.scopes }, false);
	if (response) {
		ws.send(`/p ${obj.resourceId} ${response}`);
	}
}

type Datagram = {
	name: string;
	value: string;
	scopes: string[];
	createdAt: number;
	keepFor: number;
};

export type CacheInfo = {
	resourceName: string;
	resourceScopes: string[];
	keepResourceFor: number;
	waitForResource?: number;
}

async function readDatagram(info: CacheInfo, checkDate = true): Promise<string | null> {
	const db = await openDB()
	return new Promise(resolve => {
		const tx = db.transaction(STORE_NAME, 'readwrite');
		const store = tx.objectStore(STORE_NAME);
		const request = store.get(info.resourceName);

		request.onsuccess = () => {
			const result = request.result as Datagram | undefined
			if (!result) {
				return resolve(null)
			}
			if (!result.keepFor) {
				store.delete(info.resourceName);
				return resolve(null);
			}
			if (checkDate && (Date.now() - result.createdAt) > result.keepFor) {
				store.delete(info.resourceName);
				return resolve(null);
			}
			if (result.scopes.find(scope => info.resourceScopes.includes(scope))) {
				return resolve(result.value);
			}
			return resolve(null)
		};
		request.onerror = () => resolve(null);
	})
}

async function insertDatagram(info: CacheInfo, value: string) {
	const db = await openDB();
	const tx = db.transaction(STORE_NAME, 'readwrite');
	const store = tx.objectStore(STORE_NAME);
	const datagram: Datagram = {
		createdAt: Date.now(),
		name: info.resourceName,
		scopes: info.resourceScopes,
		value: value,
		keepFor: info.keepResourceFor
	}
	store.add(datagram);
}

async function requestFromNetwork(info: CacheInfo): Promise<string | null> {
	if (ws.readyState !== WebSocket.OPEN) {
		return null;
	}

	const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

	pendingRequests.set(info.resourceName, null);
	ws.send(`/r ${info.resourceName}`);
	

	const iterations = Math.ceil(info.waitForResource! / 50)
	for (let i = 0; i < iterations; i++) {
		await delay(50);
		const response = pendingRequests.get(info.resourceName);
		if (response) {
			return response;
		}
	}

	return null;
}

export async function fetchCached(input: RequestInfo | URL, options?: RequestInit & Partial<CacheInfo>) {
	const info = {
		resourceName: options?.resourceName || new Request(input, options).url,
		resourceScopes: options?.resourceScopes || ['*'],
		keepResourceFor: options?.keepResourceFor || CACHE_DURATION,
		waitForResource: options?.waitForResource || DEFAULT_WAITING_TIME
	};

	let response = await readDatagram(info);
	if (response) {
		return new Response(response, { status: 200, headers: { 'Content-Type': 'application/json' } });
	}
	
	response = await requestFromNetwork(info);
	if (response) {
		insertDatagram(info, response);
		return new Response(response, { status: 200, headers: { 'Content-Type': 'application/json' } });
	}

	const serverResponse = await window.fetch(input, options);
	response = await serverResponse.text();
	
	if (serverResponse.status === 200) {
		insertDatagram(info, response)
	}

	return new Response(response, { status: 200, headers: { 'Content-Type': 'application/json' } });
}

function openDB(): Promise<IDBDatabase> {
	return new Promise((resolve, reject) => {
		const request = window.indexedDB.open(DB_NAME, DB_VERSION)

		request.onupgradeneeded = (event: any) => {
			const db = event.target!.result as IDBDatabase;
			if (!db.objectStoreNames.contains(STORE_NAME)) {
				db.createObjectStore(STORE_NAME, { keyPath: 'name' });
			}
		};

		request.onsuccess = (event: any) => resolve(event.target.result);
		request.onerror = (event: any) => reject(event?.target?.error);
	});
}
