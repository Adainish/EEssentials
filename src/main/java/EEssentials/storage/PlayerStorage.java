package EEssentials.storage;

import EEssentials.EEssentials;
import EEssentials.util.Location;
import EEssentials.util.cereal.LocationDeserializer;
import EEssentials.util.cereal.LocationSerializer;
import EEssentials.util.cereal.ServerWorldDeserializer;
import EEssentials.util.cereal.ServerWorldSerializer;
import com.google.common.reflect.TypeToken;
import com.google.gson.*;
import net.minecraft.server.network.ServerPlayerEntity;
import net.minecraft.server.world.ServerWorld;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class PlayerStorage {

    public final UUID playerUUID;
    private String playerName;
    public final HashMap<String, Location> homes = new HashMap<>();
    public Boolean playedBefore = false;
    public Boolean socialSpyFlag = false;
    private Location previousLocation = null;
    private Location logoutLocation = null;
    private int totalPlaytime = 0;
    private Instant lastTimeOnline = Instant.now();
    public List<String> modImports = new ArrayList<>();

    /**
     * Constructor to initialize PlayerStorage with a given UUID.
     *
     * @param uuid the UUID of the player.
     */
    public PlayerStorage(UUID uuid) {
        this.playerUUID = uuid;
        this.playedBefore = false;
        this.lastTimeOnline = Instant.now();
        this.load();
    }


    // Single-threaded executor to serialize disk writes off the main server thread
    private static final ExecutorService IO_EXECUTOR = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "EEssentials-IO-Thread");
        t.setDaemon(true);
        return t;
    });

    /**
     * Call on server shutdown to allow executor to terminate cleanly.
     */
    public static void shutdownIOExecutor() {
        IO_EXECUTOR.shutdown();
        try {
            if (!IO_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                IO_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            IO_EXECUTOR.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }


    /**
     * Fetch player storage for a given online player entity.
     *
     * @param player the player entity.
     * @return the PlayerStorage instance for the player.
     */
    public static PlayerStorage fromPlayer(ServerPlayerEntity player) {
        PlayerStorage storage = new PlayerStorage(player.getUuid());
        storage.playerName = player.getName().getString();
        storage.save();
        return storage;
    }

    /**
     * Fetch player storage using a player's UUID.
     *
     * @param uuid the UUID of the player.
     * @return the PlayerStorage instance if exists, null otherwise.
     */
    public static PlayerStorage fromPlayerUUID(UUID uuid) {
        File file = EEssentials.storage.playerStorageDirectory.resolve(uuid.toString() + ".json").toFile();
        if (!file.exists()) {
            return null;  // Return null if there's no data file for the given UUID.
        }
        return new PlayerStorage(uuid);
    }

    /**
     * Get the storage file associated with the player's UUID.
     *
     * @return the storage file.
     */
    public File getSaveFile() {
        File file = EEssentials.storage.playerStorageDirectory.resolve(playerUUID.toString() + ".json").toFile();
        playedBefore = file.exists();
        return file;
    }

    private Path getSavePath() {
        return EEssentials.storage.playerStorageDirectory.resolve(playerUUID.toString() + ".json");
    }

    /**
     * Creates a custom GSON parser with required type adapters.
     *
     * @return the custom GSON parser.
     */
    private Gson createCustomGson() {
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting();
        builder.registerTypeAdapter(ServerWorld.class, new ServerWorldSerializer());
        builder.registerTypeAdapter(ServerWorld.class, new ServerWorldDeserializer());
        builder.registerTypeAdapter(Location.class, new LocationSerializer());
        builder.registerTypeAdapter(Location.class, new LocationDeserializer());
        return builder.create();
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public UUID getPlayerUUID() {
        return this.playerUUID;
    }


    public void setPreviousLocation(Location location) {
        this.previousLocation = location;
        this.save();
    }

    public Location getPreviousLocation() {
        return this.previousLocation;
    }

    public void setLogoutLocation(Location location) {
        this.logoutLocation = location;
        this.save();
    }

    public Location getLogoutLocation() {
        return this.logoutLocation;
    }

    public int getTotalPlaytime() {
        return totalPlaytime;
    }

    public void setTotalPlaytime(int totalPlaytime) {
        this.totalPlaytime = totalPlaytime;
        this.save();
    }

    public void setLastTimeOnline() {
        this.lastTimeOnline = Instant.now();
        this.save();
    }

    public Instant getLastTimeOnline() {
        return this.lastTimeOnline;
    }

    public Boolean getSocialSpyFlag() {
        return this.socialSpyFlag;
    }

    /**
     * Save player data to storage asynchronously to avoid blocking the server thread.
     * Writes to a temporary file and then atomically replaces the real file to avoid truncation/corruption.
     */
    public void save() {
        // Capture snapshot of fields needed for serialization to avoid races
        final String playerNameSnapshot = playerName;
        final HashMap<String, Location> homesSnapshot = new HashMap<>(homes);
        final Location previousLocationSnapshot = previousLocation;
        final Location logoutLocationSnapshot = logoutLocation;
        final int totalPlaytimeSnapshot = totalPlaytime;
        final Instant lastTimeOnlineSnapshot = lastTimeOnline;
        final Boolean socialSpyFlagSnapshot = socialSpyFlag;
        final List<String> modImportsSnapshot = new ArrayList<>(modImports);
        final Path savePath = getSavePath();
        final Path tempPath = savePath.resolveSibling(savePath.getFileName().toString() + ".tmp");
        final Gson gson = createCustomGson();

        IO_EXECUTOR.submit(() -> {
            try {
                // Ensure directory exists
                Files.createDirectories(savePath.getParent());

                // Write JSON to temp file
                try (BufferedWriter writer = Files.newBufferedWriter(tempPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.addProperty("playerName", playerNameSnapshot);
                    jsonObject.add("homes", gson.toJsonTree(homesSnapshot));
                    jsonObject.add("previousLocation", gson.toJsonTree(previousLocationSnapshot));
                    jsonObject.add("logoutLocation", gson.toJsonTree(logoutLocationSnapshot));
                    jsonObject.addProperty("totalPlaytime", totalPlaytimeSnapshot);
                    jsonObject.add("lastTimeOnline", gson.toJsonTree(lastTimeOnlineSnapshot != null ? lastTimeOnlineSnapshot.toString() : Instant.now().toString()));
                    jsonObject.addProperty("socialSpyFlag", socialSpyFlagSnapshot);
                    jsonObject.add("modImports", gson.toJsonTree(modImportsSnapshot));
                    gson.toJson(jsonObject, writer);
                }

                // Atomically replace target with temp. If ATOMIC_MOVE unsupported, fallback to non-atomic replace.
                try {
                    Files.move(tempPath, savePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tempPath, savePath, StandardCopyOption.REPLACE_EXISTING);
                }

                // mark that a file exists now
                playedBefore = true;
            } catch (IOException e) {
                EEssentials.LOGGER.error("Failed to save data for UUID: " + playerUUID.toString(), e);
                // Attempt to remove temp file if present
                try {
                    Files.deleteIfExists(tempPath);
                } catch (IOException ignored) {
                }
            }
        });
    }

    public void load() {
        Gson gson = createCustomGson();
        File saveFile = getSaveFile();

        // If file does not exist, treat as new player; will be created on save()
        if (!saveFile.exists()) {
            playedBefore = false;
            return;
        }

        try (Reader reader = new FileReader(saveFile)) {
            JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);

            // Check if jsonObject is null and if so, initialize it
            if (jsonObject == null) {
                jsonObject = new JsonObject();
            }

            boolean requiresSave = false; // Flag to indicate if the data should be saved after loading

            // Load player name
            if (jsonObject.has("playerName")) {
                playerName = jsonObject.get("playerName").getAsString();
            } else {
                playerName = "Unknown"; // or some other default value
                requiresSave = true;    // Set the flag to save since playerName was missing
            }

            // Load homes data if available
            if (jsonObject.has("homes")) {
                HashMap<String, Location> loadedHomes = gson.fromJson(jsonObject.get("homes"), new TypeToken<HashMap<String, Location>>() {
                }.getType());
                if (loadedHomes != null) {
                    homes.clear();
                    homes.putAll(loadedHomes);
                }
            }

            if (jsonObject.has("previousLocation")) {
                previousLocation = gson.fromJson(jsonObject.get("previousLocation"), Location.class);
            } else {
                requiresSave = true;
            }

            if (jsonObject.has("logoutLocation")) {
                logoutLocation = gson.fromJson(jsonObject.get("logoutLocation"), Location.class);
            } else {
                requiresSave = true;
            }

            if (jsonObject.has("totalPlaytime")) {
                totalPlaytime = jsonObject.get("totalPlaytime").getAsInt();
            } else {
                totalPlaytime = 0;  // Default playtime for older files
                requiresSave = true;
            }

            if (jsonObject.has("lastTimeOnline")) {
                lastTimeOnline = Instant.parse(jsonObject.get("lastTimeOnline").getAsString());
            } else {
                lastTimeOnline = Instant.now();
                requiresSave = true;
            }

            // Load or initialize socialSpyFlag
            if (jsonObject.has("socialSpyFlag")) {
                socialSpyFlag = jsonObject.get("socialSpyFlag").getAsBoolean();
            } else {
                socialSpyFlag = false; // Default value if the flag isn't present
                requiresSave = true;
            }

            // Track MODIDs of mods that we've already imported data from for this player
            if (jsonObject.has("modImports")) {
                modImports.clear();
                modImports = gson.fromJson(jsonObject.get("modImports"), new TypeToken<ArrayList<String>>() {
                }.getType());
            }

            if (requiresSave) {
                save();
            }

        } catch (JsonParseException | NullPointerException e) {
            // Malformed JSON: back up corrupt file and start fresh
            try {
                Path path = saveFile.toPath();
                Path corruptBackup = path.resolveSibling(path.getFileName().toString() + ".corrupt." + Instant.now().toEpochMilli());
                Files.move(path, corruptBackup, StandardCopyOption.REPLACE_EXISTING);
                EEssentials.LOGGER.warn("Corrupt player data detected for UUID " + playerUUID + ". Backed up to " + corruptBackup.getFileName());
            } catch (IOException ex) {
                EEssentials.LOGGER.warn("Failed to backup corrupt player data for UUID " + playerUUID, ex);
            }
            // Reset fields to defaults and schedule a save to recreate a clean file
            playerName = playerName == null ? "Unknown" : playerName;
            homes.clear();
            previousLocation = null;
            logoutLocation = null;
            totalPlaytime = 0;
            lastTimeOnline = Instant.now();
            socialSpyFlag = false;
            modImports.clear();
            save();
        } catch (IOException e) {
            EEssentials.LOGGER.info("Failed to load data from file: " + saveFile.getName(), e);
        }
    }

}
