package EEssentials.commands.other;

import EEssentials.lang.LangManager;
import EEssentials.screens.InventoryScreen;
import com.mojang.authlib.GameProfile;
import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.context.CommandContext;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import me.lucko.fabric.api.permissions.v0.Permissions;
import net.fabricmc.fabric.api.entity.FakePlayer;
import net.minecraft.command.argument.GameProfileArgumentType;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.screen.ScreenHandlerType;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.command.CommandManager;
import net.minecraft.server.command.ServerCommandSource;
import net.minecraft.server.network.ServerPlayerEntity;
import net.minecraft.text.Text;
import net.minecraft.util.Formatting;

import java.util.Optional;

public class InvseeCommand {

    private static final String INVSEE_EDIT_PERMISSION_NODE = "eessentials.invsee.edit";
    private static final String INVSEE_VIEW_PERMISSION_NODE = "eessentials.invsee.view";

    public static void register(CommandDispatcher<ServerCommandSource> dispatcher) {
        dispatcher.register(CommandManager.literal("invsee")
                .requires(src -> Permissions.check(src, INVSEE_EDIT_PERMISSION_NODE, 2) ||
                        Permissions.check(src, INVSEE_VIEW_PERMISSION_NODE, 2))
                .then(CommandManager.argument("target", GameProfileArgumentType.gameProfile())
                        .executes(ctx -> openInventory(ctx))));
    }

    private static int openInventory(CommandContext<ServerCommandSource> ctx) throws CommandSyntaxException {
        ServerPlayerEntity player = ctx.getSource().getPlayer();
        GameProfile targetProfile = GameProfileArgumentType.getProfileArgument(ctx, "target").iterator().next();
        ServerPlayerEntity targetPlayer = ctx.getSource().getServer().getPlayerManager().getPlayer(targetProfile.getName());

        boolean isOffline = false;

        if (targetPlayer == null) {
            targetPlayer = FakePlayer.get(ctx.getSource().getWorld(), targetProfile);
            Optional<NbtCompound> optionalNbt = ctx.getSource().getServer().getPlayerManager().loadPlayerData(targetPlayer);

            if (optionalNbt.isEmpty()) {
                LangManager.send(ctx.getSource(), "Invalid-Player");
                return 0;
            }

            isOffline = true;
        }

        if (player.getUuid().equals(targetPlayer.getUuid())) {
            LangManager.send(ctx.getSource(), "Invalid-Self-Target");
            return 0;
        }

        boolean canEdit = Permissions.check(player, INVSEE_EDIT_PERMISSION_NODE, 2);

        InventoryScreen gui = new InventoryScreen(ScreenHandlerType.GENERIC_9X6, player, targetPlayer);
        gui.setTitle(
                Text.literal("\uE001\uE000\uE002")
                        .styled(style -> style.withColor(Formatting.WHITE)) // enforce white colour
                        .append(Text.literal(targetPlayer.getName().getString() + "'s Inventory").styled(style -> style.withColor(Formatting.DARK_GRAY)))
        );
        gui.setEditMode(canEdit);
        // If offline, save the data when the GUI is closed
        if (isOffline && canEdit) {
            final ServerPlayerEntity finalTarget = targetPlayer;
            final MinecraftServer server = ctx.getSource().getServer();

            gui.setCloseAction(v -> {
                // 1. Check if the player joined while we were editing
                if (server.getPlayerManager().getPlayer(finalTarget.getUuid()) == null) {
                    // 2. Explicitly write the FakePlayer's inventory to the .dat file
                    server.getPlayerManager().savePlayerData(finalTarget);
                } else {
                    // Notify the admin that the save was skipped because the player is now online
                    player.sendMessage(Text.literal("Target player logged in; changes were not saved to disk to prevent corruption.")
                            .formatted(Formatting.RED));
                }
            });
        }
        gui.open();

        return 1;
    }
}
