import { DEFAULT_NAMESPACE_FOR_INDEX_STORE } from "../constants";
import { MongoDBKVStore } from "../kvStore/MongoKVStore";
import { KVIndexStore } from "./KVIndexStore";

export class MongoIndexStore extends KVIndexStore {
  constructor(
    store: MongoDBKVStore,
    namespace: string = DEFAULT_NAMESPACE_FOR_INDEX_STORE,
  ) {
    super(store, namespace);
  }

  static fromUri(uri: string, dbName?: string, namespace?: string) {
    const mongoKVStore = MongoDBKVStore.fromUri(uri, dbName);
    return new MongoIndexStore(mongoKVStore, namespace);
  }
}
