public class SingleFlight<T> {

    private final ConcurrentHashMap<String, CompletableFuture<T>> flights =
            new ConcurrentHashMap<>();

    public T execute(String key, Supplier<T> supplier) {
        CompletableFuture<T> future = new CompletableFuture<>();
        CompletableFuture<T> existing = flights.putIfAbsent(key, future);

        // follower
        if (existing != null) {
            return existing.join();
        }

        // leader
        try {
            T result = supplier.get();
            future.complete(result);
            return result;
        } catch (Throwable t) {
            future.completeExceptionally(t);
            throw new CompletionException(t);
        } finally {
            flights.remove(key, future); // ⚠️ CAS remove，必须
        }
    }
}
