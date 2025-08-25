const { Client, GatewayIntentBits, SlashCommandBuilder, PermissionFlagsBits, ChannelType, EmbedBuilder, ActionRowBuilder, ButtonBuilder, ButtonStyle } = require('discord.js');
const cron = require('node-cron');
const { Pool, neonConfig } = require('@neondatabase/serverless');
const { drizzle } = require('drizzle-orm/neon-serverless');
const { eq, and, lt, desc } = require('drizzle-orm');
const { pgTable, text, timestamp, integer, json, boolean } = require('drizzle-orm/pg-core');
const ws = require('ws');

// Configure Neon serverless
neonConfig.webSocketConstructor = ws;

// Optimized helper function to get member name with caching and fallback
async function getMemberName(guild, userId) {
    try {
        // Check cache first (performance optimization)
        const cacheKey = `${guild.id}-${userId}`;
        const cachedName = memberNameCache.get(cacheKey);
        if (cachedName && Date.now() - cachedName.timestamp < 300000) { // 5 minute cache
            return cachedName.name;
        }

        // Try guild cache
        let member = guild.members.cache.get(userId);
        if (member) {
            const name = member.displayName;
            memberNameCache.set(cacheKey, { name, timestamp: Date.now() });
            return name;
        }

        // Fetch from Discord API with timeout protection
        const fetchPromise = guild.members.fetch(userId);
        const timeoutPromise = new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Fetch timeout')), 5000)
        );
        
        member = await Promise.race([fetchPromise, timeoutPromise]);
        const name = member.displayName;
        memberNameCache.set(cacheKey, { name, timestamp: Date.now() });
        return name;
    } catch (error) {
        // Optimized fallback with better error handling
        if (error.message !== 'Fetch timeout') {
            console.warn(`Could not fetch member name for ${userId}:`, error.message);
        }
        return `Player-${userId.slice(-4)}`;
    }
}

// Environment Variables Validation
const requiredEnvVars = ['DATABASE_URL', 'DISCORD_TOKEN'];
const missingEnvVars = requiredEnvVars.filter(envVar => !process.env[envVar]);

if (missingEnvVars.length > 0) {
    console.error('❌ Missing required environment variables:', missingEnvVars);
    process.exit(1);
}

// Optimized database setup with connection pooling
// Clean up DATABASE_URL if it contains the psql command prefix
let connectionString = process.env.DATABASE_URL;
if (connectionString.startsWith('psql ')) {
    // Extract the connection string from psql command format
    const match = connectionString.match(/'([^']+)'/);
    if (match) {
        connectionString = match[1];
    }
}

// Enhanced connection pool with performance optimizations
const pool = new Pool({
    connectionString,
    max: 20, // Maximum number of clients in pool
    idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
    connectionTimeoutMillis: 5000, // Return error if connection takes longer than 5 seconds
    query_timeout: 10000, // Query timeout of 10 seconds
    keepAlive: true,
    keepAliveInitialDelayMillis: 10000
});

// Add pool error handling for stability
pool.on('error', (err) => {
    console.error('❌ Unexpected database pool error:', err);
});

// Database schema definitions
const lfgSessions = pgTable('lfg_sessions', {
    id: text('id').primaryKey(),
    creatorId: text('creator_id').notNull(),
    guildId: text('guild_id').notNull(),
    channelId: text('channel_id').notNull(),
    messageId: text('message_id'),
    game: text('game').notNull(),
    gamemode: text('gamemode').notNull(),
    playersNeeded: integer('players_needed').notNull(),
    info: text('info'),
    status: text('status').notNull().default('waiting'),
    currentPlayers: json('current_players').notNull().default([]),
    confirmedPlayers: json('confirmed_players').notNull().default([]),
    voiceChannelId: text('voice_channel_id'),
    confirmationStartTime: timestamp('confirmation_start_time'),
    createdAt: timestamp('created_at').notNull().defaultNow(),
    updatedAt: timestamp('updated_at').notNull().defaultNow(),
    expiresAt: timestamp('expires_at').notNull(),
    isActive: boolean('is_active').notNull().default(true)
});

const guildSettings = pgTable('guild_settings', {
    guildId: text('guild_id').primaryKey(),
    lfgChannelId: text('lfg_channel_id'),
    createdAt: timestamp('created_at').notNull().defaultNow(),
    updatedAt: timestamp('updated_at').notNull().defaultNow()
});

const userSessions = pgTable('user_sessions', {
    userId: text('user_id').primaryKey(),
    sessionId: text('session_id').notNull(),
    createdAt: timestamp('created_at').notNull().defaultNow(),
    updatedAt: timestamp('updated_at').notNull().defaultNow()
});

const db = drizzle(pool, {
    schema: { lfgSessions, guildSettings, userSessions }
});

// Auto-create tables on startup for deployment environments
async function ensureDatabaseTables() {
    try {
        console.log('🔧 Checking database connection and tables...');

        // Test database connection first
        await pool.query('SELECT 1');
        console.log('✅ Database connection verified');

        // Create tables if they don't exist (safe for deployment)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS lfg_sessions (
                id TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,
                guild_id TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                message_id TEXT,
                game TEXT NOT NULL,
                gamemode TEXT NOT NULL,
                players_needed INTEGER NOT NULL,
                info TEXT,
                status TEXT NOT NULL DEFAULT 'waiting',
                current_players JSON NOT NULL DEFAULT '[]',
                confirmed_players JSON NOT NULL DEFAULT '[]',
                voice_channel_id TEXT,
                confirmation_start_time TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMP NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT true
            );
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id TEXT PRIMARY KEY,
                lfg_channel_id TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        `);

        console.log('✅ Database tables verified/created successfully');
    } catch (error) {
        console.error('❌ Database setup failed:', error);
        console.error('🔍 Check your DATABASE_URL environment variable');
        throw error;
    }
}

// Database storage class
class DatabaseStorage {
    async createSession(session) {
        try {
            const [createdSession] = await db
                .insert(lfgSessions)
                .values({
                    ...session,
                    expiresAt: new Date(Date.now() + 20 * 60 * 1000),
                    updatedAt: new Date()
                })
                .returning();
            return createdSession;
        } catch (error) {
            console.error('❌ Database error creating session:', error);
            throw error;
        }
    }

    async getSession(sessionId) {
        try {
            const [session] = await db
                .select()
                .from(lfgSessions)
                .where(and(eq(lfgSessions.id, sessionId), eq(lfgSessions.isActive, true)));
            return session || undefined;
        } catch (error) {
            console.error('❌ Database error getting session:', error);
            return undefined;
        }
    }

    async updateSession(sessionId, updates) {
        try {
            const [updatedSession] = await db
                .update(lfgSessions)
                .set({ ...updates, updatedAt: new Date() })
                .where(and(eq(lfgSessions.id, sessionId), eq(lfgSessions.isActive, true)))
                .returning();
            return updatedSession || undefined;
        } catch (error) {
            console.error('❌ Database error updating session:', error);
            return undefined;
        }
    }

    async deleteSession(sessionId) {
        try {
            const [deletedSession] = await db
                .update(lfgSessions)
                .set({ isActive: false, updatedAt: new Date() })
                .where(eq(lfgSessions.id, sessionId))
                .returning();
            return !!deletedSession;
        } catch (error) {
            console.error('❌ Database error deleting session:', error);
            return false;
        }
    }

    async getAllActiveSessions() {
        try {
            const sessions = await db
                .select()
                .from(lfgSessions)
                .where(eq(lfgSessions.isActive, true))
                .orderBy(desc(lfgSessions.createdAt));
            return sessions;
        } catch (error) {
            console.error('❌ Database error getting all sessions:', error);
            return [];
        }
    }

    async getGuildSettings(guildId) {
        try {
            const [settings] = await db
                .select()
                .from(guildSettings)
                .where(eq(guildSettings.guildId, guildId));
            return settings || undefined;
        } catch (error) {
            console.error('❌ Database error getting guild settings:', error);
            return undefined;
        }
    }

    async setGuildSettings(guildId, lfgChannelId) {
        try {
            const [settings] = await db
                .insert(guildSettings)
                .values({ guildId, lfgChannelId, updatedAt: new Date() })
                .onConflictDoUpdate({
                    target: guildSettings.guildId,
                    set: { lfgChannelId, updatedAt: new Date() }
                })
                .returning();
            return settings;
        } catch (error) {
            console.error('❌ Database error setting guild settings:', error);
            return undefined;
        }
    }

    async cleanupExpiredSessions() {
        try {
            const result = await db
                .update(lfgSessions)
                .set({ isActive: false, updatedAt: new Date() })
                .where(and(
                    eq(lfgSessions.isActive, true),
                    lt(lfgSessions.expiresAt, new Date())
                ))
                .returning();
            return result.length;
        } catch (error) {
            console.error('❌ Database error cleaning expired sessions:', error);
            return 0;
        }
    }

    async createUserSession(userSession) {
        try {
            // First delete any existing user session
            await this.deleteUserSession(userSession.userId);

            const [createdUserSession] = await db
                .insert(userSessions)
                .values({ ...userSession, updatedAt: new Date() })
                .returning();
            return createdUserSession;
        } catch (error) {
            console.error('❌ Database error creating user session:', error);
            throw error;
        }
    }

    async getUserSession(userId) {
        try {
            const [userSession] = await db
                .select()
                .from(userSessions)
                .where(eq(userSessions.userId, userId));
            return userSession || undefined;
        } catch (error) {
            console.error('❌ Database error getting user session:', error);
            return undefined;
        }
    }

    async deleteUserSession(userId) {
        try {
            await db
                .delete(userSessions)
                .where(eq(userSessions.userId, userId));
            return true;
        } catch (error) {
            console.error('❌ Database error deleting user session:', error);
            return false;
        }
    }
}

const storage = new DatabaseStorage();

const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildVoiceStates
    ]
});

// Supported games from the images
const SUPPORTED_GAMES = {
    'valorant': { name: 'Valorant', modes: ['Competitive', 'Unrated', 'Spike Rush', 'Deathmatch'] },
    'fortnite': { name: 'Fortnite', modes: ['Battle Royale', 'Zero Build', 'Creative', 'Save the World'] },
    'brawlhalla': { name: 'Brawlhalla', modes: ['1v1', '2v2', 'Ranked', 'Experimental'] },
    'thefinals': { name: 'The Finals', modes: ['Quick Cash', 'Bank It', 'Tournament'] },
    'roblox': { name: 'Roblox', modes: ['Various', 'Roleplay', 'Simulator', 'Obby'] },
    'minecraft': { name: 'Minecraft', modes: ['Survival', 'Creative', 'PvP', 'Minigames'] },
    'marvelrivals': { name: 'Marvel Rivals', modes: ['Quick Match', 'Competitive', 'Custom'] },
    'rocketleague': { name: 'Rocket League', modes: ['3v3', '2v2', '1v1', 'Hoops'] },
    'apexlegends': { name: 'Apex Legends', modes: ['Trios', 'Duos', 'Ranked', 'Arenas'] },
    'callofduty': { name: 'Call of Duty', modes: ['Multiplayer', 'Warzone', 'Search & Destroy'] },
    'overwatch': { name: 'Overwatch', modes: ['Competitive', 'Quick Play', 'Arcade'] },
    'amongus': { name: 'Among Us', modes: ['Classic', 'Hide and Seek', 'Custom Rules', 'Private Lobby'] }
};

// 💾 PERSISTENT STORAGE - Sessions survive bot restarts!
const activeSessions = new Map();
const gameCategories = new Map();
const guildSettingsCache = new Map(); // Cache for guild settings
const userCreatedSessions = new Map(); // Track which user created which session
const voiceChannelOperations = new Map(); // Prevent race conditions in voice channel operations
const interactionTimeouts = new Map(); // Track interaction timeouts to prevent expired responses
const memberNameCache = new Map(); // Cache member names for performance
const guildSettingsLoadTime = new Map(); // Track when guild settings were last loaded

// 💾 PERSISTENT SESSION MANAGEMENT - Survives bot restarts!
async function saveSessionData() {
    try {
        // Sessions are automatically saved to database on every change
        const sessionCount = activeSessions.size;
        console.log(`💾 ${sessionCount} sessions currently in memory and synced to database`);
        return sessionCount;
    } catch (error) {
        console.error('❌ Error checking session data:', error);
        return 0;
    }
}

async function loadSessionData() {
    try {
        console.log('💾 Loading persistent sessions from database...');

        // Clear existing memory state to ensure clean restoration
        activeSessions.clear();
        userCreatedSessions.clear();
        guildSettingsCache.clear();

        // Load all active sessions from database
        const dbSessions = await storage.getAllActiveSessions();
        console.log(`📋 Found ${dbSessions.length} active sessions in database`);

        // Load guild settings for all guilds with performance tracking
        try {
            const allGuilds = client.guilds.cache.values();
            const loadStart = Date.now();
            
            for (const guild of allGuilds) {
                const guildSettings = await storage.getGuildSettings(guild.id);
                if (guildSettings) {
                    guildSettingsCache.set(guild.id, {
                        lfgChannel: guildSettings.lfgChannelId
                    });
                    guildSettingsLoadTime.set(guild.id, Date.now());
                }
            }
            
            const loadTime = Date.now() - loadStart;
            console.log(`📋 Loaded settings for ${guildSettingsCache.size} guilds in ${loadTime}ms`);
        } catch (error) {
            console.error('⚠️ Error loading guild settings:', error);
        }

        // Restore sessions to memory
        let restoredCount = 0;
        let expiredCount = 0;

        for (const dbSession of dbSessions) {
            try {
                // Check if session is still valid (not expired)
                if (new Date(dbSession.expiresAt) > new Date()) {
                    // Validate that guild and channel still exist
                    const guild = client.guilds.cache.get(dbSession.guildId);
                    const channel = guild?.channels.cache.get(dbSession.channelId);

                    if (!guild || !channel) {
                        console.log(`🧹 Cleaning up session #${dbSession.id.slice(-6)} - guild/channel no longer exists`);
                        await storage.deleteSession(dbSession.id);

                        // Also cleanup any orphaned voice channel
                        if (guild && dbSession.voiceChannelId) {
                            try {
                                const voiceChannel = guild.channels.cache.get(dbSession.voiceChannelId);
                                if (voiceChannel) {
                                    await safeDeleteVoiceChannel(voiceChannel, 'orphaned_session_cleanup');
                                }
                            } catch (error) {
                                console.error(`⚠️ Error cleaning orphaned voice channel:`, error);
                            }
                        }
                        continue;
                    }

                    // Convert database session to memory format
                    const session = {
                        id: dbSession.id,
                        creator: dbSession.creatorId,
                        guildId: dbSession.guildId,
                        channelId: dbSession.channelId,
                        messageId: dbSession.messageId,
                        game: dbSession.game,
                        gamemode: dbSession.gamemode,
                        playersNeeded: dbSession.playersNeeded,
                        info: dbSession.info,
                        status: dbSession.status,
                        currentPlayers: Array.isArray(dbSession.currentPlayers) ? dbSession.currentPlayers : [],
                        confirmedPlayers: Array.isArray(dbSession.confirmedPlayers) ? dbSession.confirmedPlayers : [],
                        voiceChannel: dbSession.voiceChannelId,
                        confirmationStartTime: dbSession.confirmationStartTime ? new Date(dbSession.confirmationStartTime).getTime() : null,
                        createdAt: new Date(dbSession.createdAt).getTime(),
                        timeoutId: null // Will be restored by session management
                    };

                    activeSessions.set(dbSession.id, session);

                    // Track creator sessions - CRITICAL for endlfg command
                    if (dbSession.creatorId) {
                        userCreatedSessions.set(dbSession.creatorId, dbSession.id);
                    }

                    // Restore session timeouts for automatic expiration
                    const timeRemaining = new Date(dbSession.expiresAt).getTime() - Date.now();
                    if (timeRemaining > 0) {
                        session.timeoutId = setTimeout(async () => {
                            await expireSession(dbSession.id, 'timeout');
                        }, timeRemaining);
                    }

                    restoredCount++;
                } else {
                    // Clean up expired sessions
                    await storage.deleteSession(dbSession.id);
                    expiredCount++;
                    console.log(`🧹 Cleaned up expired session #${dbSession.id.slice(-6)}`);
                }
            } catch (sessionError) {
                console.error(`❌ Error restoring session #${dbSession.id.slice(-6)}:`, sessionError);
                // Mark problematic session as inactive
                try {
                    await storage.deleteSession(dbSession.id);
                } catch (deleteError) {
                    console.error(`❌ Failed to clean up problematic session:`, deleteError);
                }
            }
        }

        console.log(`✅ Session restoration complete:`);
        console.log(`   🔄 Restored: ${restoredCount} active sessions`);
        console.log(`   🧹 Cleaned: ${expiredCount} expired sessions`);
        if (restoredCount > 0) {
            console.log('🎆 Session persistence working perfectly - no data lost!');
        }

        // Run immediate cleanup of any remaining expired sessions
        await storage.cleanupExpiredSessions();

    } catch (error) {
        console.error('❌ Critical error loading session data from database:', error);
        console.log('⚠️ Bot will continue with empty session state');
        // Clear maps to ensure consistent state
        activeSessions.clear();
        userCreatedSessions.clear();
    }
}

// 🔄 Enhanced session cleanup and synchronization
setInterval(async () => {
    try {
        if (activeSessions.size > 0) {
            await saveSessionData();
            console.log(`🔄 Database sync: ${activeSessions.size} sessions active and persistent`);
        }

        // Clean up expired sessions from database
        const cleanedCount = await storage.cleanupExpiredSessions();
        if (cleanedCount > 0) {
            console.log(`🧹 Cleaned ${cleanedCount} expired sessions from database`);
        }

        // Clean up stale voice channel operations tracking
        const now = Date.now();
        for (const [channelId, timestamp] of voiceChannelOperations.entries()) {
            if (now - timestamp > 30000) { // 30 seconds timeout
                voiceChannelOperations.delete(channelId);
            }
        }
    } catch (error) {
        console.error('❌ Error in session synchronization:', error);
    }
}, 5 * 60 * 1000);

// Function to handle session expiration
async function expireSession(sessionId, reason = 'expired') {
    try {
        const session = activeSessions.get(sessionId);
        if (!session) {
            console.log(`⚠️ Cannot expire session #${sessionId.slice(-6)} - not found in memory`);
            return;
        }

        console.log(`⏰ Expiring session #${sessionId.slice(-6)} - reason: ${reason}`);

        // Get guild and channel info
        const guild = client.guilds.cache.get(session.guildId);
        const channel = guild?.channels.cache.get(session.channelId);

        // Delete voice channel and cleanup category if empty
        try {
            const voiceChannel = guild?.channels.cache.get(session.voiceChannel);
            if (voiceChannel) {
                const category = voiceChannel.parent;
                await voiceChannel.delete();
                console.log(`🗑️ Deleted voice channel for expired session ${sessionId}`);

                // Immediately check and cleanup empty category
                if (category && category.name.startsWith('🎮') && category.children.cache.size === 0) {
                    await category.delete();
                    console.log(`🗑️ Deleted empty category: ${category.name}`);
                }
            }
        } catch (error) {
            console.error('Error deleting voice channel during expiration:', error);
        }

        // Create simple expired embed - clean and clear
        const expiredEmbed = new EmbedBuilder()
            .setColor(0x95a5a6) // Gray for expired
            .setTitle('⏰ **Session Expired**')
            .setDescription(`**${session.game} session has automatically ended**\n\n🔄 Create a new session with \`/lfg\``)
            .addFields(
                {
                    name: '📋 Session Info',
                    value: `**Game:** ${session.game}\n**Mode:** ${session.gamemode}\n**Status:** ❌ Expired\n**Duration:** ${getTimeAgo(session.createdAt)}`,
                    inline: false
                }
            )
            .setFooter({ 
                text: `Session #${sessionId.slice(-6)} • Expired after 20 minutes`
            })
            .setTimestamp();

        // Update original message if possible
        if (channel && session.messageId) {
            try {
                const originalMessage = await channel.messages.fetch(session.messageId);
                await originalMessage.edit({ embeds: [expiredEmbed], components: [] });
                console.log(`✅ Updated expired session message for #${sessionId.slice(-6)}`);
            } catch (error) {
                console.warn(`⚠️ Could not update expired session message: ${error.message}`);
            }
        }

        // Notify session owner via DM
        if (session.creator) {
            try {
                const creator = await client.users.fetch(session.creator);
                if (creator) {
                    const dmEmbed = new EmbedBuilder()
                        .setColor(0x95a5a6)
                        .setTitle('⏰ Your LFG Session Expired')
                        .setDescription(`Your **${session.game}** session has automatically ended after 20 minutes.`)
                        .addFields({
                            name: '📋 Session Details',
                            value: `**Game:** ${session.game}\n**Mode:** ${session.gamemode}\n**Players:** ${session.currentPlayers.length}/${session.playersNeeded}\n**Duration:** ${getTimeAgo(session.createdAt)}`,
                            inline: false
                        })
                        .setFooter({ text: `Session #${sessionId.slice(-6)} • Create a new one with /lfg` })
                        .setTimestamp();
                    
                    await creator.send({ embeds: [dmEmbed] });
                    console.log(`📧 Sent expiration notification to session creator ${creator.displayName || creator.username}`);
                }
            } catch (dmError) {
                console.log(`⚠️ Could not send DM to session creator: ${dmError.message}`);
            }
        }

        // Clear timeout if it exists
        if (session.timeoutId) {
            clearTimeout(session.timeoutId);
            session.timeoutId = null;
        }

        // Remove from memory and database
        activeSessions.delete(sessionId);

        // Clean up user session tracking for all players, not just creator
        if (session.creator) {
            userCreatedSessions.delete(session.creator);
        }

        // Also clean up any other user session references
        for (const [userId, userSessionId] of userCreatedSessions.entries()) {
            if (userSessionId === sessionId) {
                userCreatedSessions.delete(userId);
            }
        }

        // Remove all user session mappings from database
        for (const playerId of session.currentPlayers || []) {
            try {
                await storage.deleteUserSession(playerId);
            } catch (dbError) {
                console.error(`Failed to delete user session for ${playerId}:`, dbError);
            }
        }

        try {
            await storage.deleteSession(sessionId);
            console.log(`💾 Expired session #${sessionId.slice(-6)} removed from database`);
        } catch (dbError) {
            console.error(`❌ Failed to remove expired session from database:`, dbError);
        }

    } catch (error) {
        console.error(`❌ Error expiring session #${sessionId.slice(-6)}:`, error);
    }
}

// Create consistent detailed LFG embed format (original beautiful style)
function createDetailedLfgEmbed(session, guild, sessionId) {
    const spotsLeft = session.playersNeeded - session.currentPlayers.length;
    const isFull = spotsLeft === 0;

    // Create visual progress bar
    const progressBar = createProgressBar(session.currentPlayers.length, session.playersNeeded);

    const embed = new EmbedBuilder()
        .setColor(0x00d4ff)
        .setTitle(`🎮 ${session.game} • Looking for Group`)
        .setDescription(isFull ? 
            '🎯 **Party is full!** Waiting for confirmations...' : 
            `🔍 **Seeking ${spotsLeft} skilled ${spotsLeft === 1 ? 'player' : 'players'} to complete the squad**`)
        .addFields(
            { 
                name: '👥 Party Progress', 
                value: `${progressBar}\n**${session.currentPlayers.length}/${session.playersNeeded} players**`, 
                inline: false 
            },
            { 
                name: '🎯 Current Squad', 
                value: session.currentPlayers.length > 0 
                    ? session.currentPlayers.map((playerId, index) => {
                        const member = guild.members.cache.get(playerId);
                        const playerName = member?.displayName || `Player-${playerId.slice(-4)}`;
                        const icon = index === 0 ? '👑' : '⚔️'; // Leader gets crown, others get sword
                        return `${icon} **${playerName}**`;
                    }).join('\n')
                    : '🔍 **Waiting for players to join...**', 
                inline: false 
            },
            { 
                name: '🎮 Game Details', 
                value: `**Game:** ${session.game}\n**Mode:** ${session.gamemode}\n**Skill Level:** All Welcome`, 
                inline: true 
            },
            { 
                name: '👥 Squad Status', 
                value: `**Current:** ${session.currentPlayers.length}/${session.playersNeeded}\n**Available Spots:** ${spotsLeft}\n**Status:** ${isFull ? '🔴 Full' : '🟢 Recruiting'}`, 
                inline: true 
            },
            { 
                name: '⏱️ Session Info', 
                value: `**Created:** <t:${Math.floor(session.createdAt/1000)}:R>\n**Expires:** <t:${Math.floor((session.createdAt + 1200000)/1000)}:R>\n**Region:** Global`, 
                inline: true 
            },
            { 
                name: '👑 Squad Leader', 
                value: `**${guild.members.cache.get(session.creator)?.displayName || 'Unknown'}**\n🌟 Session Creator\n🎯 Ready to Play`, 
                inline: false 
            },
            { 
                name: '🔊 Premium Voice Channel', 
                value: `<#${session.voiceChannel}>\n🔒 **Private & Secure** - Auto-access when you join\n🎤 Crystal clear voice communication\n⚡ Low latency gaming optimized`, 
                inline: false 
            }
        );

    // Add info field if provided
    if (session.info) {
        embed.addFields({ name: '📝 Additional Info', value: session.info, inline: false });
    }

    embed.setFooter({ 
        text: `Session #${sessionId.slice(-6)} • Created by ${guild.members.cache.get(session.creator)?.displayName || `Creator-${session.creator.slice(-4)}`}`,
        iconURL: guild.members.cache.get(session.creator)?.displayAvatarURL() || null
    })
    .setTimestamp();

    return embed;
}

// 🎙️ ENHANCED VOICE CHANNEL MANAGEMENT SYSTEM

// Safe voice channel deletion with race condition prevention
async function safeDeleteVoiceChannel(voiceChannel, reason = 'cleanup') {
    if (!voiceChannel || !voiceChannel.id) return false;

    const channelId = voiceChannel.id;
    const operationKey = `delete_${channelId}`;

    // Prevent race conditions
    if (voiceChannelOperations.has(operationKey)) {
        console.log(`⚠️ Voice channel deletion already in progress: ${voiceChannel.name}`);
        return false;
    }

    voiceChannelOperations.set(operationKey, Date.now());

    try {
        // Check if channel still exists and we have permission
        const channel = await voiceChannel.fetch().catch(() => null);
        if (!channel) {
            console.log(`🔍 Voice channel ${channelId} no longer exists`);
            return true; // Consider this successful
        }

        // Verify bot has permission to delete
        const botMember = channel.guild.members.cache.get(client.user.id);
        if (!botMember?.permissions.has(PermissionFlagsBits.ManageChannels)) {
            console.error(`❌ No permission to delete voice channel: ${channel.name}`);
            return false;
        }

        // Disconnect all users before deletion
        if (channel.members.size > 0) {
            console.log(`🔇 Disconnecting ${channel.members.size} users from ${channel.name}`);
            for (const member of channel.members.values()) {
                try {
                    await member.voice.disconnect(`LFG session ended - ${reason}`);
                } catch (disconnectError) {
                    console.warn(`⚠️ Could not disconnect ${member.displayName}:`, disconnectError.message);
                }
            }
        }

        // Get category before deleting the channel
        const category = channel.parent;

        // Delete the channel
        await channel.delete(`LFG ${reason}`);
        console.log(`🗑️ Successfully deleted voice channel: ${channel.name} (${reason})`);

        // Immediately check and cleanup empty category
        if (category && category.name.startsWith('🎮') && category.children.cache.size === 0) {
            await category.delete();
            console.log(`🗑️ Deleted empty category: ${category.name}`);
        }

        // Update database to remove voice channel reference
        const session = Array.from(activeSessions.values()).find(s => s.voiceChannel === channelId);
        if (session) {
            try {
                await storage.updateSession(session.id, { voiceChannelId: null });
            } catch (dbError) {
                console.warn(`⚠️ Could not update database for deleted voice channel:`, dbError);
            }
        }

        return true;

    } catch (error) {
        console.error(`❌ Error deleting voice channel ${voiceChannel.name}:`, error);
        return false;
    } finally {
        voiceChannelOperations.delete(operationKey);
    }
}

// Enhanced voice channel permission management
async function manageVoiceChannelAccess(voiceChannel, userId, action = 'grant', reason = 'LFG access') {
    if (!voiceChannel || !userId) return false;

    const operationKey = `permission_${voiceChannel.id}_${userId}`;

    // Prevent race conditions
    if (voiceChannelOperations.has(operationKey)) {
        console.log(`⚠️ Voice permission operation already in progress for user ${userId}`);
        return false;
    }

    voiceChannelOperations.set(operationKey, Date.now());

    try {
        const permissions = {
            Connect: action === 'grant',
            ViewChannel: action === 'grant',
            Speak: action === 'grant'
        };

        if (action === 'grant') {
            await voiceChannel.permissionOverwrites.edit(userId, permissions, { reason });
            console.log(`✅ Granted voice access to user ${userId} in ${voiceChannel.name}`);
        } else if (action === 'revoke') {
            await voiceChannel.permissionOverwrites.delete(userId, reason);
            console.log(`❌ Revoked voice access for user ${userId} from ${voiceChannel.name}`);
        }

        return true;

    } catch (error) {
        console.error(`❌ Error managing voice permissions for ${userId}:`, error);
        return false;
    } finally {
        voiceChannelOperations.delete(operationKey);
    }
}

// Enhanced voice channel creation with better error handling
async function createLfgVoiceChannel(guild, user, gameData, category) {
    try {
        // Verify bot has necessary permissions
        const botMember = guild.members.cache.get(client.user.id);
        if (!botMember?.permissions.has([PermissionFlagsBits.ManageChannels, PermissionFlagsBits.Connect])) {
            throw new Error('Bot lacks required permissions to create voice channels');
        }

        const channelName = `${gameData.name} - ${user.displayName}`.substring(0, 50); // Discord limit

        const voiceChannel = await guild.channels.create({
            name: channelName,
            type: ChannelType.GuildVoice,
            parent: category.id,
            userLimit: 10, // Reasonable limit for LFG sessions
            bitrate: guild.features.includes('VIP_REGIONS') ? 384000 : 128000, // Higher quality for premium servers
            permissionOverwrites: [
                {
                    id: guild.roles.everyone.id,
                    deny: [PermissionFlagsBits.Connect, PermissionFlagsBits.ViewChannel]
                },
                {
                    id: user.id,
                    allow: [
                        PermissionFlagsBits.Connect,
                        PermissionFlagsBits.ViewChannel,
                        PermissionFlagsBits.Speak,
                        PermissionFlagsBits.UseVAD,
                        PermissionFlagsBits.Stream
                    ]
                },
                {
                    id: client.user.id, // Bot permissions
                    allow: [
                        PermissionFlagsBits.Connect,
                        PermissionFlagsBits.ViewChannel,
                        PermissionFlagsBits.ManageChannels,
                        PermissionFlagsBits.MoveMembers,
                        PermissionFlagsBits.MuteMembers
                    ]
                }
            ]
        });

        console.log(`🎙️ Created LFG voice channel: ${channelName}`);
        return voiceChannel;

    } catch (error) {
        console.error(`❌ Error creating LFG voice channel:`, error);
        throw error;
    }
}

// Voice state change handler for better user experience
client.on('voiceStateUpdate', async (oldState, newState) => {
    try {
        const userId = newState.member?.id;
        if (!userId) return;

        // Handle user leaving LFG voice channels
        if (oldState.channel && !newState.channel) {
            const leftChannelId = oldState.channelId;
            const session = Array.from(activeSessions.values()).find(s => s.voiceChannel === leftChannelId);

            if (session && session.currentPlayers.includes(userId)) {
                console.log(`📢 User ${newState.member.displayName} left LFG voice channel`);

                // Don't automatically remove from session - let them rejoin
                // Only log for monitoring purposes
            }
        }

        // Handle user joining LFG voice channels
        if (!oldState.channel && newState.channel) {
            const joinedChannelId = newState.channelId;
            const session = Array.from(activeSessions.values()).find(s => s.voiceChannel === joinedChannelId);

            if (session && session.currentPlayers.includes(userId)) {
                console.log(`🎙️ User ${newState.member.displayName} joined LFG voice channel`);
            }
        }

    } catch (error) {
        console.error('❌ Error in voice state update handler:', error);
    }
});

client.once('clientReady', async () => {
    console.log(`🚀 Party Up! Bot is online! Logged in as ${client.user.tag}`);
    console.log(`🎮 Serving ${client.guilds.cache.size} servers with premium LFG features`);

    try {
        await loadSessionData(); // Load any persisted session data
        await registerCommands();
        console.log('🎯 Bot ready and operational!');
    } catch (error) {
        console.error('❌ Error during bot ready phase:', error);
    }

    // Enhanced cleanup system running every minute with optimized performance
    cron.schedule('* * * * *', async () => {
        console.log('🧹 Running optimized cleanup tasks...');
        
        const cleanupStart = Date.now();
        
        // Run all cleanup tasks in parallel for better performance
        const [emptyChannels, expiredConfirmations, expiredSessions, cacheCleared] = await Promise.allSettled([
            cleanupEmptyChannels(),
            checkExpiredConfirmations(), 
            checkExpiredLfgSessions(),
            cleanupCaches()
        ]);
        
        const cleanupTime = Date.now() - cleanupStart;
        
        // Log results of cleanup operations
        let cleanupResults = [];
        if (emptyChannels.status === 'rejected') {
            console.error('Error in channel cleanup:', emptyChannels.reason);
        }
        if (expiredConfirmations.status === 'rejected') {
            console.error('Error checking confirmations:', expiredConfirmations.reason);
        }
        if (expiredSessions.status === 'rejected') {
            console.error('Error checking sessions:', expiredSessions.reason);
        }
        if (cacheCleared.status === 'fulfilled' && cacheCleared.value > 0) {
            cleanupResults.push(`${cacheCleared.value} cached items`);
        }
        
        // Performance summary
        if (cleanupResults.length > 0) {
            console.log(`🧹 Cleanup completed in ${cleanupTime}ms - cleared: ${cleanupResults.join(', ')}`);
        } else {
            console.log(`🧹 Cleanup completed in ${cleanupTime}ms - system optimized`);
        }
    });

});

async function registerCommands() {
    const commands = [
        new SlashCommandBuilder()
            .setName('lfg')
            .setDescription('Look for group - find teammates for your game')
            .addStringOption(option =>
                option.setName('game')
                    .setDescription('Choose a game')
                    .setRequired(true)
                    .addChoices(
                        ...Object.entries(SUPPORTED_GAMES).map(([key, game]) => ({
                            name: game.name,
                            value: key
                        }))
                    ))
            .addStringOption(option =>
                option.setName('gamemode')
                    .setDescription('Game mode')
                    .setRequired(true)
                    .setAutocomplete(true))
            .addIntegerOption(option =>
                option.setName('players')
                    .setDescription('Number of players needed (including you)')
                    .setRequired(true)
                    .setMinValue(2)
                    .setMaxValue(10))
            .addStringOption(option =>
                option.setName('info')
                    .setDescription('Additional information (optional)')
                    .setRequired(false)
                    .setMaxLength(200)),
        new SlashCommandBuilder()
            .setName('setchannel')
            .setDescription('Set the LFG channel (Staff only)')
            .addChannelOption(option =>
                option.setName('channel')
                    .setDescription('Channel where LFG commands are allowed')
                    .setRequired(true)
                    .addChannelTypes(ChannelType.GuildText))
            .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels),
        new SlashCommandBuilder()
            .setName('embed')
            .setDescription('Create custom embed messages (Staff only)')
            .addStringOption(option =>
                option.setName('title')
                    .setDescription('Embed title')
                    .setRequired(true))
            .addStringOption(option =>
                option.setName('description')
                    .setDescription('Embed description')
                    .setRequired(true))
            .addStringOption(option =>
                option.setName('color')
                    .setDescription('Embed color (hex code like #ff0000)')
                    .setRequired(false))
            .setDefaultMemberPermissions(PermissionFlagsBits.ManageMessages),
        new SlashCommandBuilder()
            .setName('mod')
            .setDescription('Moderation commands (Staff only)')
            .addStringOption(option =>
                option.setName('action')
                    .setDescription('Moderation action')
                    .setRequired(true)
                    .addChoices(
                        { name: 'Kick', value: 'kick' },
                        { name: 'Ban', value: 'ban' },
                        { name: 'Mute', value: 'mute' },
                        { name: 'Unmute', value: 'unmute' }
                    ))
            .addUserOption(option =>
                option.setName('user')
                    .setDescription('Target user')
                    .setRequired(true))
            .addStringOption(option =>
                option.setName('reason')
                    .setDescription('Reason for action')
                    .setRequired(false))
            .addStringOption(option =>
                option.setName('duration')
                    .setDescription('Duration for mute (e.g., 10m, 1h, 1d)')
                    .setRequired(false))
            .setDefaultMemberPermissions(PermissionFlagsBits.ModerateMembers),
        new SlashCommandBuilder()
            .setName('help')
            .setDescription('Show all bot commands and features'),
        new SlashCommandBuilder()
            .setName('endlfg')
            .setDescription('End your active LFG session')
    ].map(command => command.toJSON());

    try {
        console.log('Started refreshing application (/) commands.');
        await client.application.commands.set(commands);
        console.log('Successfully reloaded application (/) commands.');
    } catch (error) {
        console.error('Error registering commands:', error);
    }
}

client.on('interactionCreate', async interaction => {
    if (interaction.isChatInputCommand()) {
        if (interaction.commandName === 'lfg') {
            await handleLfgCommand(interaction);
        } else if (interaction.commandName === 'setchannel') {
            await handleSetChannelCommand(interaction);
        } else if (interaction.commandName === 'embed') {
            await handleEmbedCommand(interaction);
        } else if (interaction.commandName === 'mod') {
            await handleModCommand(interaction);
        } else if (interaction.commandName === 'help') {
            await handleHelpCommand(interaction);
        } else if (interaction.commandName === 'endlfg') {
            await handleEndLfgCommand(interaction);
        }
        return;
    }

    if (interaction.isButton()) {
        if (interaction.customId.startsWith('join_lfg_')) {
            await handleJoinLfg(interaction);
        } else if (interaction.customId.startsWith('confirm_')) {
            await handleConfirmation(interaction);
        } else if (interaction.customId.startsWith('decline_')) {
            await handleDecline(interaction);
        } else if (interaction.customId.startsWith('leave_lfg_')) {
            await handleLeaveLfg(interaction);
        }
        return;
    }

    if (interaction.isAutocomplete()) {
        try {
            if (interaction.commandName === 'lfg') {
                const focusedOption = interaction.options.getFocused(true);

                if (focusedOption.name === 'gamemode') {
                    const game = interaction.options.getString('game');
                    if (game && SUPPORTED_GAMES[game]) {
                        const modes = SUPPORTED_GAMES[game].modes.filter(mode => 
                            mode.toLowerCase().includes(focusedOption.value.toLowerCase())
                        );
                        await interaction.respond(
                            modes.map(mode => ({ name: mode, value: mode }))
                        );
                    } else {
                        await interaction.respond([]);
                    }
                }
            }
        } catch (error) {
            console.error('Error handling autocomplete:', error);
            try {
                if (!interaction.responded) {
                    await interaction.respond([]);
                }
            } catch (respondError) {
                console.error('Error responding to autocomplete:', respondError);
            }
        }
    }
});

async function handleLfgCommand(interaction) {
    // Defer reply immediately to prevent timeout issues
    await interaction.deferReply();

    const game = interaction.options.getString('game');
    const gamemode = interaction.options.getString('gamemode');
    const playersNeeded = interaction.options.getInteger('players');
    const info = interaction.options.getString('info');
    const user = interaction.user;
    const guild = interaction.guild;

    // Check if user already has an active LFG session as creator
    // Clean up any orphaned user session references first
    const existingSessionId = userCreatedSessions.get(user.id);
    if (existingSessionId) {
        const existingSession = activeSessions.get(existingSessionId);
        if (!existingSession) {
            // Clean up orphaned reference and also check database
            userCreatedSessions.delete(user.id);
            try {
                const dbSession = await storage.getSession(existingSessionId);
                if (dbSession) {
                    await storage.deleteSession(existingSessionId);
                    console.log(`🧹 Cleaned up orphaned session #${existingSessionId.slice(-6)} from database`);
                }
            } catch (error) {
                console.error('Error cleaning up orphaned database session:', error);
            }
            console.log(`Cleaned up orphaned session reference for user ${user.displayName}`);
        } else {
            return interaction.editReply({ 
                content: `❌ You already have an active LFG session (#${existingSessionId.slice(-6)}) for ${existingSession.game}! Use \`/endlfg\` to end it first.`
            });
        }
    }

    // Check if user is already in another LFG session with better cleanup
    let userInSession = Array.from(activeSessions.values()).find(s => 
        s.currentPlayers.includes(user.id)
    );

    // If found in memory, double-check in database and clean up if needed
    if (userInSession) {
        try {
            const dbSession = await storage.getSession(userInSession.id);
            if (!dbSession || !dbSession.currentPlayers.includes(user.id)) {
                // Session doesn't exist in DB or user isn't in it - clean up memory
                const sessionIndex = userInSession.currentPlayers.indexOf(user.id);
                if (sessionIndex > -1) {
                    userInSession.currentPlayers.splice(sessionIndex, 1);
                    console.log(`🧹 Cleaned up user ${user.displayName} from stale session #${userInSession.id.slice(-6)}`);
                }
                userInSession = null; // Allow user to create new session
            } else {
                return interaction.editReply({ 
                    content: `❌ You are already in an LFG session (#${userInSession.id.slice(-6)})! Use \`/endlfg\` or the leave button to exit first.`
                });
            }
        } catch (error) {
            console.error('Error checking user session in database:', error);
            // If database check fails, still prevent creating new session for safety
            return interaction.editReply({ 
                content: `❌ You are already in an LFG session (#${userInSession.id.slice(-6)})! Use \`/endlfg\` to end it first.`
            });
        }
    }

    // Check if LFG channel is set and if user is in the correct channel
    const guildSetting = guildSettingsCache.get(guild.id);

    if (guildSetting && guildSetting.lfgChannel) {
        if (interaction.channel.id !== guildSetting.lfgChannel) {
            const lfgChannel = guild.channels.cache.get(guildSetting.lfgChannel);
            return interaction.editReply({ 
                content: `❌ LFG commands can only be used in ${lfgChannel ? lfgChannel.toString() : 'the designated channel'}!`
            });
        }
    }

    if (!SUPPORTED_GAMES[game]) {
        return interaction.editReply({ content: 'Unsupported game selected.' });
    }

    const gameData = SUPPORTED_GAMES[game];

    // Validate gamemode
    if (!gameData.modes.includes(gamemode)) {
        return interaction.editReply({ 
            content: `Invalid mode for ${gameData.name}. Available modes: ${gameData.modes.join(', ')}`
        });
    }

    try {
        // Get or create game category
        const category = await getOrCreateGameCategory(guild, game, gameData.name);

        // Create private voice channel
        const voiceChannel = await createLfgVoiceChannel(guild, user, gameData, category);

        // 💾 Create persistent LFG session with database storage
        const sessionId = `${user.id}-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`;

        // Prepare session data for database
        const sessionData = {
            id: sessionId,
            creatorId: user.id,
            guildId: guild.id,
            channelId: interaction.channel.id,
            game: gameData.name,
            gamemode: gamemode,
            playersNeeded: playersNeeded,
            info: info,
            status: 'waiting',
            currentPlayers: [user.id],
            confirmedPlayers: [],
            voiceChannelId: voiceChannel.id,
            confirmationStartTime: null
        };

        // 💾 Save to database FIRST for persistence
        let dbSession;
        try {
            dbSession = await storage.createSession(sessionData);
            console.log(`💾 Created persistent session #${sessionId.slice(-6)} in database`);

            // Create user session tracking in database
            await storage.createUserSession({
                userId: user.id,
                sessionId: sessionId
            });
            console.log(`📋 Created user session tracking for ${user.displayName}`);
        } catch (error) {
            console.error('❌ Failed to create session in database:', error);
            // Clean up voice channel if database creation failed
            if (voiceChannel) {
                try {
                    await voiceChannel.delete();
                    console.log('🧹 Cleaned up voice channel after database error');
                } catch (cleanupError) {
                    console.error('Error cleaning up voice channel:', cleanupError);
                }
            }

            await interaction.editReply({ 
                content: '❌ **Database Error**: Could not create persistent session. Please try again.'
            });
            return;
        }

        // Create memory session with all data
        const session = {
            id: sessionId,
            creator: user.id,
            guildId: guild.id,
            channelId: interaction.channel.id,
            messageId: null, // Will be set after message creation
            game: gameData.name,
            gamemode: gamemode,
            playersNeeded: playersNeeded,
            info: info,
            currentPlayers: [user.id],
            confirmedPlayers: [],
            voiceChannel: voiceChannel.id,
            category: category.id,
            createdAt: Date.now(),
            confirmationStartTime: null, // When confirmation phase started
            status: 'waiting', // waiting, confirming, completed
            timeoutId: null // Store timeout ID for proper cleanup
        };

        activeSessions.set(sessionId, session);
        userCreatedSessions.set(user.id, sessionId); // Track creator

        // Set up automatic expiration after 20 minutes
        session.timeoutId = setTimeout(async () => {
            // Only expire if session still has only the creator (no one else joined)
            const currentSession = activeSessions.get(sessionId);
            if (currentSession && currentSession.currentPlayers.length === 1 && currentSession.status === 'waiting') {
                await expireSession(sessionId, 'timeout_no_joiners');
            }
        }, 20 * 60 * 1000); // 20 minutes

        // Create embed using original beautiful detailed format
        const embed = createDetailedLfgEmbed(session, guild, sessionId);

        const joinButton = new ButtonBuilder()
            .setCustomId(`join_lfg_${sessionId}`)
            .setLabel(`⚡ Join Squad (${playersNeeded - 1} spots left)`)
            .setStyle(ButtonStyle.Success)
            .setEmoji('🎮');

        const row = new ActionRowBuilder().addComponents(joinButton);

        const response = await interaction.editReply({ embeds: [embed], components: [row] });

        // 💾 Store the message ID for reliable updates in BOTH memory and database
        session.messageId = response.id;

        // Update database with message ID for persistence
        try {
            await storage.updateSession(sessionId, { messageId: response.id });
            console.log(`💾 Updated session #${sessionId.slice(-6)} with message ID in database`);
        } catch (error) {
            console.error('❌ Failed to update message ID in database:', error);
        }

    } catch (error) {
        console.error('Error creating LFG session:', error);

        // Clean up any partially created resources
        if (userCreatedSessions.has(user.id)) {
            const partialSessionId = userCreatedSessions.get(user.id);
            activeSessions.delete(partialSessionId);
            userCreatedSessions.delete(user.id);

            // Also clean up from database
            try {
                await storage.deleteSession(partialSessionId);
                console.log(`🧹 Cleaned up partial session #${partialSessionId.slice(-6)} from database`);
            } catch (dbError) {
                console.error('Error cleaning up partial session from database:', dbError);
            }
        }

        // Clean up voice channel if it was created
        if (voiceChannel) {
            try {
                await voiceChannel.delete();
                console.log('🧹 Cleaned up voice channel after error');
            } catch (cleanupError) {
                console.error('Error cleaning up voice channel:', cleanupError);
            }
        }

        try {
            await interaction.editReply({ content: 'Failed to create LFG session. Please try again.' });
        } catch (replyError) {
            console.error('Error sending failure message:', replyError);
        }
    }
}

async function getOrCreateGameCategory(guild, gameKey, gameName) {
    const categoryName = `🎮 ${gameName}`;

    // Check if category already exists
    let category = guild.channels.cache.find(c => 
        c.type === ChannelType.GuildCategory && c.name === categoryName
    );

    if (!category) {
        // Create new category
        category = await guild.channels.create({
            name: categoryName,
            type: ChannelType.GuildCategory,
            permissionOverwrites: [
                {
                    id: guild.roles.everyone.id,
                    allow: [PermissionFlagsBits.ViewChannel],
                    deny: [PermissionFlagsBits.Connect]
                }
            ]
        });

        gameCategories.set(gameKey, category.id);
        console.log(`Created category: ${categoryName}`);
    }

    return category;
}

// Store empty channel timestamps
const emptyChannelTimestamps = new Map();

// Cleanup empty channels every minute with improved reliability
async function cleanupEmptyChannels() {
    for (const guild of client.guilds.cache.values()) {
        try {
            // Verify bot has necessary permissions
            const botMember = guild.members.cache.get(client.user.id);
            if (!botMember || !botMember.permissions.has(PermissionFlagsBits.ManageChannels)) {
                console.log(`Skipping cleanup for guild ${guild.name} - missing permissions`);
                continue;
            }
            // Clean up empty LFG voice channels - but respect active LFG sessions for 20 minutes
            const voiceChannels = guild.channels.cache.filter(c => 
                c.type === ChannelType.GuildVoice && 
                c.members.size === 0 && 
                c.parent && 
                c.parent.name.startsWith('🎮')
            );

            for (const channel of voiceChannels.values()) {
                const now = Date.now();

                // Check if this channel belongs to an active LFG session
                const session = Array.from(activeSessions.values()).find(s => s.voiceChannel === channel.id);

                if (session) {
                    // Verify session is still valid
                    if (!session.guildId || !session.creator || !session.currentPlayers) {
                        console.log(`Found corrupted session ${session.id}, removing...`);
                        activeSessions.delete(session.id);
                        userCreatedSessions.delete(session.creator);
                        // Allow channel to be cleaned up
                    } else if (session.status === 'waiting') {
                        // Active LFG session - don't delete the voice channel
                        // Voice channel will be deleted when:
                        // 1. Session times out after 20 minutes (handled by checkExpiredLfgSessions)
                        // 2. All players leave permanently (handled by leave LFG command)
                        console.log(`Protecting active LFG session voice channel: ${channel.name}`);
                        // Clear any empty timestamp since we're protecting this channel
                        emptyChannelTimestamps.delete(channel.id);
                    } else {
                        // Session in other states (started, ended, etc.) - allow normal cleanup
                        if (!emptyChannelTimestamps.has(channel.id)) {
                            // Mark channel as empty
                            emptyChannelTimestamps.set(channel.id, now);
                        } else {
                            // Check if channel has been empty for more than 5 minutes
                            const emptyTime = now - emptyChannelTimestamps.get(channel.id);
                            if (emptyTime > 300000) { // 5 minutes for non-waiting sessions
                                const category = channel.parent;
                                await channel.delete();
                                emptyChannelTimestamps.delete(channel.id);
                                console.log(`Deleted empty voice channel: ${channel.name} (session not waiting, 5min cleanup)`);

                                // Immediately check and cleanup empty category
                                if (category && category.name.startsWith('🎮') && category.children.cache.size === 0) {
                                    await category.delete();
                                    console.log(`Deleted empty category: ${category.name}`);
                                }
                            }
                        }
                    }
                } else {
                    // No active session, use regular 1-minute cleanup
                    if (!emptyChannelTimestamps.has(channel.id)) {
                        // Mark channel as empty
                        emptyChannelTimestamps.set(channel.id, now);
                    } else {
                        // Check if channel has been empty for more than 1 minute
                        const emptyTime = now - emptyChannelTimestamps.get(channel.id);
                        if (emptyTime > 60000) { // 1 minute
                            const category = channel.parent;
                            await channel.delete();
                            emptyChannelTimestamps.delete(channel.id);
                            console.log(`Deleted empty voice channel: ${channel.name} (no active session)`);

                            // Immediately check and cleanup empty category
                            if (category && category.name.startsWith('🎮') && category.children.cache.size === 0) {
                                await category.delete();
                                console.log(`Deleted empty category: ${category.name}`);
                            }
                        }
                    }
                }
            }

            // Clean up empty game categories
            const gameCategories = guild.channels.cache.filter(c => 
                c.type === ChannelType.GuildCategory && 
                c.name.startsWith('🎮') &&
                c.children.cache.size === 0
            );

            for (const category of gameCategories.values()) {
                await category.delete();
                console.log(`Deleted empty category: ${category.name}`);
            }

        } catch (error) {
            console.error('Error during cleanup:', error);
        }
    }
}

// Handle voice state changes
client.on('voiceStateUpdate', async (oldState, newState) => {
    // Handle someone leaving a channel
    if (oldState.channel && oldState.channel.members.size === 0) {
        // Channel became empty - start tracking for cleanup
        if (oldState.channel.parent && oldState.channel.parent.name.startsWith('🎮')) {
            emptyChannelTimestamps.set(oldState.channel.id, Date.now());
        }
    }

    // Handle someone joining a channel
    if (newState.channel && emptyChannelTimestamps.has(newState.channel.id)) {
        // Channel is no longer empty - stop tracking for cleanup
        emptyChannelTimestamps.delete(newState.channel.id);
    }

    // Handle joining LFG voice channels - only allow if in session
    if (newState.channel) {
        const session = Array.from(activeSessions.values()).find(s => s.voiceChannel === newState.channel.id);

        if (session) {
            // Only allow users who are part of the LFG session
            if (session.currentPlayers.includes(newState.member.id) || session.confirmedPlayers.includes(newState.member.id)) {
                console.log(`${newState.member.displayName} joined LFG voice channel: ${session.game}`);
            } else {
                // Kick users who aren't part of the session
                try {
                    await newState.member.voice.disconnect('Not part of this LFG session');
                    console.log(`Kicked ${newState.member.displayName} from LFG voice channel - not in session`);
                } catch (error) {
                    console.error('Error kicking unauthorized user:', error);
                }
            }
        }
    }

    // Handle members leaving voice - track for potential session cleanup
    if (oldState.channel && !newState.channel) {
        const session = Array.from(activeSessions.values()).find(s => s.voiceChannel === oldState.channel.id);
        if (session && session.currentPlayers.includes(oldState.member.id)) {
            console.log(`Member ${oldState.member.displayName} left LFG voice channel for session ${session.id.slice(-6)}`);
        }
    }
});

// New command handlers
async function handleSetChannelCommand(interaction) {

    if (!interaction.member.permissions.has(PermissionFlagsBits.ManageChannels)) {
        return interaction.reply({ content: '❌ You need Manage Channels permission to use this command!', flags: 64 });
    }

    const channel = interaction.options.getChannel('channel');
    const guildId = interaction.guild.id;


    // Initialize guild settings if not exists
    if (!guildSettings.has(guildId)) {
        guildSettings.set(guildId, {});
    }

    // Set the LFG channel
    const settings = guildSettings.get(guildId);
    settings.lfgChannel = channel.id;
    guildSettings.set(guildId, settings); // Make sure to set it back


    const embed = new EmbedBuilder()
        .setColor(0x00ff00)
        .setTitle('✅ LFG Channel Set')
        .setDescription(`LFG commands can now only be used in ${channel.toString()}`)
        .setTimestamp();

    await interaction.reply({ embeds: [embed] });
}

async function handleEmbedCommand(interaction) {
    if (!interaction.member.permissions.has(PermissionFlagsBits.ManageMessages)) {
        return interaction.reply({ content: '❌ You need Manage Messages permission to use this command!', flags: 64 });
    }

    const title = interaction.options.getString('title');
    const description = interaction.options.getString('description');
    const colorInput = interaction.options.getString('color') || '#5865f2';

    let color = 0x5865f2;
    if (colorInput.startsWith('#')) {
        color = parseInt(colorInput.slice(1), 16);
    }

    const embed = new EmbedBuilder()
        .setTitle(title)
        .setDescription(description)
        .setColor(color)
        .setTimestamp();

    await interaction.reply({ embeds: [embed] });
}

async function handleModCommand(interaction) {
    if (!interaction.member.permissions.has(PermissionFlagsBits.ModerateMembers)) {
        return interaction.reply({ content: '❌ You need Moderate Members permission to use this command!', flags: 64 });
    }

    const action = interaction.options.getString('action');
    const targetUser = interaction.options.getMember('user');
    const reason = interaction.options.getString('reason') || 'No reason provided';
    const duration = interaction.options.getString('duration');

    if (!targetUser) {
        return interaction.reply({ content: '❌ User not found in this server!', flags: 64 });
    }

    if (targetUser.roles.highest.position >= interaction.member.roles.highest.position) {
        return interaction.reply({ content: '❌ You cannot moderate this user (equal or higher role)!', flags: 64 });
    }

    try {
        switch (action) {
            case 'kick':
                await targetUser.kick(reason);
                await removeMemberFromAllSessions(targetUser.id);
                await interaction.reply(`✅ Kicked ${targetUser.user.tag} - ${reason}`);
                break;

            case 'ban':
                await targetUser.ban({ reason, deleteMessageDays: 1 });
                await removeMemberFromAllSessions(targetUser.id);
                await interaction.reply(`✅ Banned ${targetUser.user.tag} - ${reason}`);
                break;

            case 'mute':
                const timeoutDuration = parseDuration(duration || '1h');
                await targetUser.timeout(timeoutDuration, reason);
                await interaction.reply(`✅ Muted ${targetUser.user.tag} for ${duration || '1h'} - ${reason}`);
                break;

            case 'unmute':
                await targetUser.timeout(null, reason);
                await interaction.reply(`✅ Unmuted ${targetUser.user.tag} - ${reason}`);
                break;
        }
    } catch (error) {
        console.error('Moderation error:', error);
        await interaction.reply({ content: `❌ Failed to ${action} user: ${error.message}`, flags: 64 });
    }
}

async function handleJoinLfg(interaction) {
    try {
        // Defer the interaction immediately to prevent timeout
        await interaction.deferUpdate();

        const sessionId = interaction.customId.replace('join_lfg_', '');
        const session = activeSessions.get(sessionId);

        console.log(`User ${interaction.user.displayName} (${interaction.user.id}) attempting to join session ${sessionId}`);

        if (!session) {
            console.log(`Session ${sessionId} not found`);

            // Try to find and disable this outdated button
            const embed = new EmbedBuilder()
                .setColor(0x95a5a6)
                .setTitle('❌ LFG Session Expired')
                .setDescription('This LFG session is no longer active.')
                .setTimestamp();

            try {
                await interaction.editReply({ embeds: [embed], components: [] });
                console.log(`Disabled outdated LFG button for session ${sessionId}`);
            } catch (updateError) {
                console.error('Error disabling outdated button:', updateError);
            }
            return;
        }

        if (session.currentPlayers.includes(interaction.user.id)) {
            // User is already in this session, show enhanced status
            const statusEmbed = new EmbedBuilder()
                .setColor(0xffa500)
                .setTitle('ℹ️ **Already in Team!**')
                .setDescription(`**You're already part of this ${session.game} session**\n\n👥 **Team:** ${session.currentPlayers.length}/${session.playersNeeded}\n🔊 **Voice:** <#${session.voiceChannel}>\n⏰ **Status:** ${session.status === 'confirming' ? 'Waiting for confirmations' : 'Looking for more players'}\n\n*Click **Leave Team** if you want to exit this session.*`)
                .setTimestamp();

            const leaveButton = new ButtonBuilder()
                .setCustomId(`leave_lfg_${sessionId}`)
                .setLabel('Leave Team')
                .setStyle(ButtonStyle.Danger)
                .setEmoji('🚪');

            const row = new ActionRowBuilder().addComponents(leaveButton);

            console.log(`User ${interaction.user.id} (${interaction.user.displayName}) tried to join session they're already in`);

            return interaction.editReply({ 
                embeds: [statusEmbed],
                components: [row]
            });
        }

        if (session.currentPlayers.length >= session.playersNeeded) {
            const fullEmbed = new EmbedBuilder()
                .setColor(0xff9900)
                .setTitle('🚫 **Team is Full!**')
                .setDescription(`**This ${session.game} session is already complete**\n\n👥 **Team:** ${session.currentPlayers.length}/${session.playersNeeded}\n🎮 **Status:** ${session.status === 'confirming' ? 'Players confirming' : 'Team filled'}\n\n🔍 Try creating your own LFG with \`/lfg\`!`)
                .setTimestamp();

            return interaction.editReply({ embeds: [fullEmbed] });
        }

        // Check if user is already in another LFG session (improved check)
        const userInOtherSession = Array.from(activeSessions.values()).find(s => 
            s.id !== sessionId && s.currentPlayers.includes(interaction.user.id)
        );

        if (userInOtherSession) {
            const conflictEmbed = new EmbedBuilder()
                .setColor(0xff6b6b)
                .setTitle('⚠️ **Already in Another Team!**')
                .setDescription(`**You can only be in one LFG session at a time**\n\n🎮 **Current Session:** ${userInOtherSession.game}\n🆔 **Session ID:** #${userInOtherSession.id.slice(-6)}\n👥 **Team:** ${userInOtherSession.currentPlayers.length}/${userInOtherSession.playersNeeded}\n\n🚪 Leave your current session first to join this one!`)
                .setTimestamp();

            return interaction.editReply({ 
                embeds: [conflictEmbed]
            });
        }

        // Add user to session and update database immediately
        session.currentPlayers.push(interaction.user.id);

        // Update database with new player
        try {
            await storage.updateSession(sessionId, {
                currentPlayers: session.currentPlayers,
                status: session.status
            });
            console.log(`✅ Updated session ${sessionId.slice(-6)} in database with new player`);

            // Create user session tracking for the new player
            await storage.createUserSession({
                userId: interaction.user.id,
                sessionId: sessionId
            });
            console.log(`📋 Created user session tracking for ${interaction.user.displayName}`);
        } catch (dbError) {
            console.error(`❌ Failed to update session in database:`, dbError);
        }

    // Grant voice channel access to the new player
    try {
        const voiceChannel = interaction.guild.channels.cache.get(session.voiceChannel);
        if (voiceChannel) {
            const accessGranted = await manageVoiceChannelAccess(
                voiceChannel, 
                interaction.user.id, 
                'grant', 
                `Joined LFG session #${sessionId.slice(-6)}`
            );

            if (!accessGranted) {
                console.warn(`⚠️ Failed to grant voice access to ${interaction.user.displayName}`);
                // Still continue with the session join, voice access can be retried
            }
        }
    } catch (error) {
        console.error('Error granting voice channel access:', error);
    }

    // Keep the original format and just add party progress
    const isFull = session.currentPlayers.length === session.playersNeeded;
    const spotsLeft = session.playersNeeded - session.currentPlayers.length;

    // Use the original beautiful detailed format
    const embed = createDetailedLfgEmbed(session, interaction.guild, sessionId);

    if (session.currentPlayers.length === session.playersNeeded) {
        // Team is full, start enhanced confirmation process
        session.status = 'confirming';
        session.confirmationStartTime = Date.now();

        const confirmButton = new ButtonBuilder()
            .setCustomId(`confirm_${sessionId}`)
            .setLabel('Ready to Play!')
            .setStyle(ButtonStyle.Success)
            .setEmoji('🎮');

        const declineButton = new ButtonBuilder()
            .setCustomId(`decline_${sessionId}`)
            .setLabel('Not Available')
            .setStyle(ButtonStyle.Danger)
            .setEmoji('❌');

        const row = new ActionRowBuilder().addComponents(confirmButton, declineButton);

        // Enhanced confirmation message with better visuals
        const playerPings = session.currentPlayers.map(id => `<@${id}>`).join(' ');
        const confirmEmbed = new EmbedBuilder()
            .setColor(0xffd700)
            .setTitle('🎯 **TEAM ASSEMBLED!**')
            .setDescription(`**All players found for ${session.game}!**\n\n⏰ **You have 5 minutes to confirm**\nClick **Ready to Play!** if you're available right now.\n\n🔊 Voice channel: <#${session.voiceChannel}>`)
            .setTimestamp();

        try {
            await interaction.editReply({ embeds: [embed], components: [row] });

            // Use a separate channel message instead of followUp to avoid interaction conflicts
            const channel = interaction.guild.channels.cache.get(session.channelId);
            if (channel) {
                await channel.send({ 
                    content: `${playerPings}`,
                    embeds: [confirmEmbed],
                    allowedMentions: { users: session.currentPlayers }
                });
            }
        } catch (interactionError) {
            console.error('Error updating interaction for full team:', interactionError);
        }

        // Clear any existing timeout first to prevent conflicts
        if (session.timeoutId) {
            clearTimeout(session.timeoutId);
        }

        session.timeoutId = setTimeout(() => handleConfirmationTimeout(sessionId), 300000); // Increased to 5 minutes
        console.log(`Started confirmation timeout for session ${sessionId} at ${new Date().toISOString()}`);

        // Update session status in database
        try {
            await storage.updateSession(sessionId, {
                status: 'confirming',
                confirmationStartTime: new Date(session.confirmationStartTime),
                currentPlayers: session.currentPlayers
            });
        } catch (dbError) {
            console.error(`Failed to update session status in database:`, dbError);
        }
    } else {
        const joinButton = new ButtonBuilder()
            .setCustomId(`join_lfg_${sessionId}`)
            .setLabel(`Join Team (${spotsLeft} spots left)`)
            .setStyle(ButtonStyle.Primary)
            .setEmoji('⚡');

        const row = new ActionRowBuilder().addComponents(joinButton);

        await interaction.editReply({ embeds: [embed], components: [row] });

        // Enhanced join confirmation with better UX (only for non-full teams)
        const successEmbed = new EmbedBuilder()
            .setColor(0x00ff00)
            .setTitle('🎉 Welcome to the Team!')
            .setDescription(`**Successfully joined ${session.game}!**\n\n🔊 **Voice Channel:** <#${session.voiceChannel}>\n🎮 **Mode:** ${session.gamemode}\n👥 **Team Size:** ${session.currentPlayers.length}/${session.playersNeeded}\n\n🔍 **Waiting for ${spotsLeft} more ${spotsLeft === 1 ? 'player' : 'players'}**`)
            .setTimestamp();

        const leaveButton = new ButtonBuilder()
            .setCustomId(`leave_lfg_${sessionId}`)
            .setLabel('🚪 Leave Squad')
            .setStyle(ButtonStyle.Danger)
            .setEmoji('🔄');

        const leaveRow = new ActionRowBuilder().addComponents(leaveButton);

        // Send join confirmation as ephemeral message (only visible to the user who joined)
        try {
            await interaction.followUp({ 
                embeds: [successEmbed], 
                components: [leaveRow],
                ephemeral: true
            });
        } catch (followUpError) {
            console.error('Error sending join confirmation:', followUpError);
            // Fallback to channel message if followUp fails
            try {
                const channel = interaction.guild.channels.cache.get(session.channelId);
                if (channel) {
                    await channel.send({ 
                        content: `<@${interaction.user.id}>`,
                        embeds: [successEmbed], 
                        components: [leaveRow],
                        allowedMentions: { users: [interaction.user.id] }
                    });
                }
            } catch (channelError) {
                console.error('Error sending fallback join confirmation:', channelError);
            }
        }
    }
    } catch (error) {
        console.error('Error in handleJoinLfg:', error);
        try {
            await interaction.editReply({ 
                content: '❌ Something went wrong. Please try again.'
            });
        } catch (replyError) {
            console.error('Error replying to interaction:', replyError);
        }
    }
}

async function handleConfirmation(interaction) {
    // Add interaction timeout protection
    const interactionId = `${interaction.id}_${Date.now()}`;
    interactionTimeouts.set(interactionId, setTimeout(() => {
        interactionTimeouts.delete(interactionId);
        console.warn(`⚠️ Confirmation interaction ${interaction.id} timed out`);
    }, 14000)); // Discord interactions expire after 15 seconds

    await interaction.deferReply({ flags: 64 });

    const sessionId = interaction.customId.replace('confirm_', '');
    const session = activeSessions.get(sessionId);
    
    // Clear timeout since we're processing
    const timeout = interactionTimeouts.get(interactionId);
    if (timeout) {
        clearTimeout(timeout);
        interactionTimeouts.delete(interactionId);
    }

    if (!session) {
        const expiredEmbed = new EmbedBuilder()
            .setColor(0x95a5a6)
            .setTitle('⏰ **Session Expired**')
            .setDescription('This LFG session is no longer active or has been completed.\n\n🆕 Create a new session with `/lfg`')
            .setTimestamp();
        return interaction.editReply({ embeds: [expiredEmbed] });
    }

    if (!session.currentPlayers.includes(interaction.user.id)) {
        const notInSessionEmbed = new EmbedBuilder()
            .setColor(0xff6b6b)
            .setTitle('❌ **Not in Team**')
            .setDescription('You are not part of this LFG session!\n\n🔍 Look for open sessions or create your own with `/lfg`')
            .setTimestamp();
        return interaction.editReply({ embeds: [notInSessionEmbed] });
    }

    if (session.status !== 'confirming') {
        const notConfirmingEmbed = new EmbedBuilder()
            .setColor(0xffa500)
            .setTitle('⚠️ **Not in Confirmation Phase**')
            .setDescription('This session is not currently asking for confirmations.\n\n⏰ Wait for the team to fill up!')
            .setTimestamp();
        return interaction.editReply({ embeds: [notConfirmingEmbed] });
    }

    if (session.confirmedPlayers.includes(interaction.user.id)) {
        const alreadyConfirmedEmbed = new EmbedBuilder()
            .setColor(0x00ff00)
            .setTitle('✅ **Already Confirmed!**')
            .setDescription(`You've already confirmed for this ${session.game} session!\n\n⏰ Waiting for other players to confirm...`)
            .setTimestamp();
        return interaction.editReply({ embeds: [alreadyConfirmedEmbed] });
    }

    session.confirmedPlayers.push(interaction.user.id);

    if (session.confirmedPlayers.length === session.currentPlayers.length) {
        // All players confirmed - clear timeout and finalize
        if (session.timeoutId) {
            clearTimeout(session.timeoutId);
            session.timeoutId = null;
        }
        session.confirmationStartTime = null; // Clear confirmation time
        console.log(`All players confirmed for session ${sessionId}, finalizing`);
        await finalizeSession(session, interaction);
    } else {
        await interaction.editReply({ content: '✅ Confirmed! Waiting for other players...' });
    }
}

async function handleDecline(interaction) {
    // Add interaction timeout protection
    const interactionId = `${interaction.id}_${Date.now()}`;
    interactionTimeouts.set(interactionId, setTimeout(() => {
        interactionTimeouts.delete(interactionId);
        console.warn(`⚠️ Decline interaction ${interaction.id} timed out`);
    }, 14000));

    await interaction.deferReply({ flags: 64 });

    const sessionId = interaction.customId.replace('decline_', '');
    const session = activeSessions.get(sessionId);
    
    // Clear timeout since we're processing
    const timeout = interactionTimeouts.get(interactionId);
    if (timeout) {
        clearTimeout(timeout);
        interactionTimeouts.delete(interactionId);
    }

    if (!session || !session.currentPlayers.includes(interaction.user.id)) {
        return interaction.editReply({ content: '❌ You are not part of this LFG!' });
    }

    // Check if the session creator is declining - if so, cancel entire session
    if (interaction.user.id === session.creator) {
        console.log(`Session creator ${interaction.user.displayName} declined session ${sessionId}, cancelling entire session`);

        // Clear timeout
        if (session.timeoutId) {
            clearTimeout(session.timeoutId);
            session.timeoutId = null;
        }

        // Delete voice channel
        try {
            const voiceChannel = interaction.guild.channels.cache.get(session.voiceChannel);
            if (voiceChannel) {
                await voiceChannel.delete();
            }
        } catch (error) {
            console.error('Error deleting voice channel:', error);
        }

        // Remove session and clean up all user references
        activeSessions.delete(sessionId);
        userCreatedSessions.delete(session.creator);

        // Remove all player references from user sessions
        for (const playerId of session.currentPlayers || []) {
            userCreatedSessions.delete(playerId);
            try {
                await storage.deleteUserSession(playerId);
            } catch (dbError) {
                console.error(`Failed to delete user session for ${playerId}:`, dbError);
            }
        }

        // Remove session from database
        try {
            await storage.deleteSession(sessionId);
        } catch (dbError) {
            console.error(`Failed to delete session from database:`, dbError);
        }

        // Update embed to show session cancelled
        const cancelledEmbed = new EmbedBuilder()
            .setColor(0xff6b6b)
            .setTitle('❌ LFG Session Cancelled')
            .setDescription(`The session creator cancelled this LFG.`)
            .setTimestamp();

        await interaction.update({ embeds: [cancelledEmbed], components: [] });
        await interaction.editReply({ content: '❌ You cancelled your LFG session.' });
        return;
    }

    // Regular player declining - remove them and continue
    session.currentPlayers = session.currentPlayers.filter(id => id !== interaction.user.id);
    session.confirmedPlayers = session.confirmedPlayers.filter(id => id !== interaction.user.id);

    // Clear timeout if it exists (someone declined, so we're reopening)
    if (session.timeoutId) {
        clearTimeout(session.timeoutId);
        session.timeoutId = null;
    }
    session.confirmationStartTime = null; // Clear confirmation time
    console.log(`Player ${interaction.user.displayName} declined session ${sessionId}, reopening`);

    // Remove voice channel access
    try {
        const voiceChannel = interaction.guild.channels.cache.get(session.voiceChannel);
        if (voiceChannel) {
            await voiceChannel.permissionOverwrites.delete(interaction.user.id);
            // Disconnect if user is in the voice channel
            if (interaction.member.voice.channel?.id === session.voiceChannel) {
                await interaction.member.voice.disconnect('Declined LFG session');
            }
        }
    } catch (error) {
        console.error('Error removing voice channel access:', error);
    }

    await interaction.editReply({ content: '❌ You declined the LFG session.' });

    // Reopen LFG for remaining spots
    await reopenLfg(session);
}

async function handleConfirmationTimeout(sessionId) {
    const session = activeSessions.get(sessionId);

    if (!session || session.status !== 'confirming') {
        console.log(`Timeout called for session ${sessionId} but session not found or not confirming`);
        return;
    }

    console.log(`Processing confirmation timeout for session ${sessionId}`);

    // Clear the timeout reference
    session.timeoutId = null;

    // Get players who didn't confirm
    const unconfirmedPlayers = session.currentPlayers.filter(id => !session.confirmedPlayers.includes(id));
    console.log(`Unconfirmed players: ${unconfirmedPlayers.length}, Confirmed players: ${session.confirmedPlayers.length}`);

    // Remove voice channel access from unconfirmed players
    try {
        const guild = client.guilds.cache.get(session.guildId);
        const voiceChannel = guild?.channels.cache.get(session.voiceChannel);

        if (voiceChannel) {
            for (const playerId of unconfirmedPlayers) {
                await voiceChannel.permissionOverwrites.delete(playerId);

                // Disconnect if user is in the voice channel
                let member = guild.members.cache.get(playerId);
                if (!member) {
                    try {
                        member = await guild.members.fetch(playerId);
                    } catch (fetchError) {
                        console.warn(`Could not fetch member ${playerId} for disconnect`);
                    }
                }

                if (member && member.voice.channel?.id === session.voiceChannel) {
                    await member.voice.disconnect('Failed to confirm in time');
                }
            }
        }
    } catch (error) {
        console.error('Error removing voice access from unconfirmed players:', error);
    }

    // Always keep the creator + all confirmed players
    const confirmedPlayersSet = new Set(session.confirmedPlayers);
    const keepPlayers = [session.creator, ...session.confirmedPlayers];

    // Remove duplicates (in case creator also confirmed)
    session.currentPlayers = [...new Set(keepPlayers)];

    console.log(`Keeping creator + ${session.confirmedPlayers.length} confirmed players = ${session.currentPlayers.length} total players`);

    session.confirmedPlayers = [];
    session.status = 'waiting';
    session.confirmationStartTime = null; // Reset confirmation time

    await reopenLfg(session, null);
}

// 🕐 Enhanced session timer update system - runs every 5 minutes
async function updateSessionTimers() {
    if (activeSessions.size === 0) {
        return;
    }

    console.log(`🔄 Updating timers for ${activeSessions.size} active sessions...`);
    let updatedCount = 0;

    for (const [sessionId, session] of activeSessions) {
        try {
            if (session.status === 'waiting' && session.channelId && session.messageId) {
                const timeInfo = getDetailedExpiryTime(session.createdAt);

                // Update the session embed with new timing
                await updateSessionEmbed(sessionId, session, timeInfo);
                updatedCount++;

                console.log(`✅ Updated timer for session #${sessionId.slice(-6)}: ${timeInfo.text}`);
            }
        } catch (error) {
            console.error(`❌ Failed to update timer for session #${sessionId.slice(-6)}:`, error.message);

            // If it's a message-related error, clean up the session
            if (error.code === 10008 || error.message.includes('Unknown Message')) {
                console.warn(`🧹 Session #${sessionId.slice(-6)} has invalid message, cleaning up...`);
                await cleanupInvalidSession(sessionId, 'timer_update_failed');
            }
        }
    }

    if (updatedCount > 0) {
        console.log(`🏆 Timer update cycle completed - ${updatedCount} sessions updated`);
    }
}

// Clean up invalid sessions (deleted messages, missing channels, etc)
async function cleanupInvalidSession(sessionId, reason) {
    try {
        console.log(`🧹 Cleaning up invalid session #${sessionId.slice(-6)} (reason: ${reason})`);

        const session = activeSessions.get(sessionId);
        if (session) {
            // Clear any timeouts
            if (session.timeoutId) {
                clearTimeout(session.timeoutId);
                session.timeoutId = null;
            }

            // Remove from memory
            activeSessions.delete(sessionId);

            // Clean up ALL user session references for this session
            for (const [userId, userSessionId] of userCreatedSessions.entries()) {
                if (userSessionId === sessionId) {
                    userCreatedSessions.delete(userId);
                }
            }

            // Remove user session mappings from database
            for (const playerId of session.currentPlayers) {
                try {
                    await storage.deleteUserSession(playerId);
                } catch (dbError) {
                    console.warn(`⚠️ Could not clean up user session for ${playerId}:`, dbError.message);
                }
            }

            // Clean up voice channel if it exists
            try {
                const guild = client.guilds.cache.get(session.guildId);
                if (guild && session.voiceChannel) {
                    const voiceChannel = guild.channels.cache.get(session.voiceChannel);
                    if (voiceChannel) {
                        await safeDeleteVoiceChannel(voiceChannel, reason);
                    }
                }
            } catch (voiceError) {
                console.warn(`⚠️ Could not clean up voice channel:`, voiceError.message);
            }

            // Mark as inactive in database
            await storage.deleteSession(sessionId);
        }

        console.log(`✅ Successfully cleaned up invalid session #${sessionId.slice(-6)}`);
    } catch (error) {
        console.error(`❌ Error cleaning up invalid session #${sessionId.slice(-6)}:`, error);
    }
}

// Optimized cache cleanup function for memory management
async function cleanupCaches() {
    try {
        let itemsCleared = 0;
        const now = Date.now();
        const maxAge = 300000; // 5 minutes
        
        // Clean expired member name cache
        for (const [key, cached] of memberNameCache.entries()) {
            if (now - cached.timestamp > maxAge) {
                memberNameCache.delete(key);
                itemsCleared++;
            }
        }
        
        // Clean expired interaction timeouts
        for (const [key, timeout] of interactionTimeouts.entries()) {
            if (timeout && typeof timeout === 'object') {
                clearTimeout(timeout);
                interactionTimeouts.delete(key);
                itemsCleared++;
            }
        }
        
        // Clean stale voice channel operations (already running in setInterval, but double-check)
        for (const [channelId, timestamp] of voiceChannelOperations.entries()) {
            if (now - timestamp > 30000) { // 30 seconds timeout
                voiceChannelOperations.delete(channelId);
                itemsCleared++;
            }
        }
        
        // Clean guild settings cache older than 30 minutes
        const oldGuildSettingsAge = 30 * 60 * 1000; // 30 minutes
        for (const [guildId, timestamp] of guildSettingsLoadTime.entries()) {
            if (now - timestamp > oldGuildSettingsAge) {
                guildSettingsCache.delete(guildId);
                guildSettingsLoadTime.delete(guildId);
                itemsCleared++;
            }
        }
        
        return itemsCleared;
    } catch (error) {
        console.error('❌ Error during cache cleanup:', error);
        return 0;
    }
}

// Enhanced session embed updater with time synchronization
async function updateSessionEmbed(sessionId, session, timeInfo) {
    try {
        const guild = client.guilds.cache.get(session.guildId);
        if (!guild) return;

        const channel = guild.channels.cache.get(session.channelId);
        if (!channel) return;

        // Enhanced message fetching with proper error handling and recovery
        let message;
        try {
            if (!session.messageId) {
                console.warn(`⚠️ No message ID stored for session #${sessionId.slice(-6)}, skipping update`);
                return;
            }

            message = await channel.messages.fetch(session.messageId);
        } catch (fetchError) {
            if (fetchError.code === 10008) { // Unknown Message - message was deleted
                console.warn(`⚠️ Message deleted for session #${sessionId.slice(-6)}, attempting recovery...`);

                // Try to find the message by searching recent messages
                try {
                    const recentMessages = await channel.messages.fetch({ limit: 50 });
                    const sessionMessage = recentMessages.find(msg => 
                        msg.embeds.length > 0 && 
                        msg.embeds[0].footer?.text?.includes(sessionId.slice(-6))
                    );

                    if (sessionMessage) {
                        session.messageId = sessionMessage.id;
                        await storage.updateSession(sessionId, { messageId: sessionMessage.id });
                        console.log(`✅ Recovered message ID for session #${sessionId.slice(-6)}`);
                        message = sessionMessage;
                    } else {
                        console.log(`✅ Recovered and updated LFG message for session #${sessionId.slice(-6)}`);
                        return;
                    }
                } catch (recoveryError) {
                    console.error('Failed to recover message ID:', recoveryError);
                    return;
                }
            } else {
                throw fetchError; // Re-throw other errors
            }
        }

        if (!message) return;

        const spotsLeft = session.playersNeeded - session.currentPlayers.length;
        const isFull = spotsLeft === 0;
        const gameEmoji = getGameEmoji(session.game);
        const gameDesc = getGameDescription(session.game);

        // Use the consistent detailed embed format that includes player names
        const embed = createDetailedLfgEmbed(session, guild, sessionId);

        // Update message with new embed (keep existing components)
        await message.edit({ embeds: [embed], components: message.components });

    } catch (error) {
        console.error(`❌ Error updating session embed:`, error);
    }
}

// Backup function to check for expired confirmations (runs every minute)
async function checkExpiredConfirmations() {
    const now = Date.now();
    const twoMinutes = 2 * 60 * 1000; // 2 minutes in milliseconds

    for (const [sessionId, session] of activeSessions) {
        if (session.status === 'confirming' && session.confirmationStartTime) {
            const elapsed = now - session.confirmationStartTime;

            if (elapsed >= twoMinutes) {
                console.log(`⏰ Found expired confirmation for session #${sessionId.slice(-6)}, processing timeout`);
                await handleConfirmationTimeout(sessionId);
            }
        }
    }
}

async function checkExpiredLfgSessions() {
    const now = Date.now();
    const twentyMinutes = 20 * 60 * 1000; // 20 minutes in milliseconds

    for (const [sessionId, session] of activeSessions) {
        // Only check sessions that are in 'waiting' status with only the creator (no one joined)
        if (session.status === 'waiting' && session.currentPlayers.length === 1) {
            const elapsed = now - session.createdAt;

            if (elapsed >= twentyMinutes) {
                console.log(`⏰ Found expired LFG session #${sessionId.slice(-6)} with no joiners, processing timeout`);
                await handleLfgTimeout(sessionId);
            }
        }
    }
}

async function handleLfgTimeout(sessionId) {
    // Use the new unified expiration function
    await expireSession(sessionId, 'timeout_no_joiners');
}

async function reopenLfg(session) {
    if (session.currentPlayers.length === 0) {
        // No one left, delete session
        try {
            const guild = client.guilds.cache.get(session.guildId);
            const channel = guild?.channels.cache.get(session.voiceChannel);
            if (channel) {
                const category = channel.parent;
                await channel.delete();

                // Immediately check and cleanup empty category
                if (category && category.name.startsWith('🎮') && category.children.cache.size === 0) {
                    await category.delete();
                    console.log(`Deleted empty category: ${category.name}`);
                }
            }
        } catch (error) {
            console.error('Error deleting voice channel:', error);
        }
        activeSessions.delete(session.id);
        userCreatedSessions.delete(session.creator); // Clean up creator tracking
        return;
    }

    session.status = 'waiting';

    // Get guild reliably using stored guild ID
    const guild = client.guilds.cache.get(session.guildId);
    if (!guild) {
        console.error(`Guild not found for session ${session.id}`);
        return;
    }

    const embed = createDetailedLfgEmbed(session, guild, session.id);

    const joinButton = new ButtonBuilder()
        .setCustomId(`join_lfg_${session.id}`)
        .setLabel('Join LFG')
        .setStyle(ButtonStyle.Primary)
        .setEmoji('✅');

    const row = new ActionRowBuilder().addComponents(joinButton);

    // Use the stored channel where the original LFG was posted
    const channel = guild.channels.cache.get(session.channelId);

    if (channel) {
        let messageUpdated = false;

        // Try to update the original message first with validation
        try {
            if (session.messageId && session.messageId.length > 0) {
                try {
                    const originalMessage = await channel.messages.fetch(session.messageId);
                    await originalMessage.edit({ embeds: [embed], components: [row] });
                    console.log(`Updated original LFG message for reopened session ${session.id}`);
                    messageUpdated = true;
                } catch (fetchError) {
                    if (fetchError.code === 10008) {
                        console.log(`⚠️ Message ${session.messageId} no longer exists, clearing stored ID`);
                        session.messageId = null;
                        await storage.updateSession(session.id, { messageId: null });
                    } else {
                        throw fetchError;
                    }
                }
            }

            if (!messageUpdated) {
                // Fallback: try to find the message if no ID stored
                const messages = await channel.messages.fetch({ limit: 50 });
                const originalMessage = messages.find(msg => 
                    msg.embeds.length > 0 && 
                    msg.embeds[0].footer?.text?.includes(session.id.slice(-6))
                );

                if (originalMessage) {
                    await originalMessage.edit({ embeds: [embed], components: [row] });
                    console.log(`Updated LFG message using fallback search for reopened session ${session.id}`);
                    messageUpdated = true;
                }
            }
        } catch (error) {
            console.error('Error updating original message:', error);

            // If message fetch failed, try broader search to find and update the button
            try {
                const messages = await channel.messages.fetch({ limit: 100 });
                const originalMessage = messages.find(msg => 
                    msg.embeds.length > 0 && 
                    msg.embeds[0].footer?.text?.includes(session.id.slice(-6)) &&
                    (msg.components.length === 0 || 
                     msg.components[0].components.some(comp => comp.customId?.includes(`join_lfg_${session.id}`)))
                );

                if (originalMessage) {
                    await originalMessage.edit({ embeds: [embed], components: [row] });
                    console.log(`Found and updated LFG message using extended search for reopened session ${session.id}`);
                    messageUpdated = true;
                }
            } catch (secondError) {
                console.error('Extended search also failed:', secondError);
            }
        }

        // If we couldn't update the original message, send a new one as last resort
        if (!messageUpdated) {
            try {
                const response = await channel.send({ 
                    content: `${session.currentPlayers.map(id => `<@${id}>`).join(' ')} **A player left your LFG session!**\n\nYour team is looking for **${session.playersNeeded - session.currentPlayers.length} more player(s)** to complete the squad.`,
                    embeds: [embed], 
                    components: [row],
                    allowedMentions: { users: session.currentPlayers }
                });
                // Update the session with new message ID
                session.messageId = response.id;
                console.log(`Sent new reopened message for session ${session.id} since original couldn't be updated`);
            } catch (sendError) {
                console.error('Error sending new reopened message:', sendError);
            }
        }
    } else {
        console.error(`No suitable channel found to reopen LFG ${session.id}`);
    }
}

async function finalizeSession(session, interaction) {
    session.status = 'active';
    const gameEmoji = getGameEmoji(session.game);

    // Create spectacular final embed
    const embed = new EmbedBuilder()
        .setColor(0x00ff00)
        .setTitle(`${gameEmoji} **GAME ON!** ${gameEmoji}`)
        .setDescription(`**🎆 ${session.game} match confirmed! 🎆**\n\n✨ All players are ready - time to dominate!`)
        .addFields(
            { 
                name: '🎮 Game Session Details', 
                value: `**Game:** ${session.game}\n**Mode:** ${session.gamemode}\n**Players:** ${session.confirmedPlayers.length}\n**Status:** 🟢 Active`, 
                inline: true 
            },
            { 
                name: '🔊 Voice Communication', 
                value: `**Channel:** <#${session.voiceChannel}>\n**Access:** ✅ Granted to all\n**Ready:** Join now!`, 
                inline: true 
            },
            { 
                name: '🏆 Your Team', 
                value: session.confirmedPlayers.map((id, index) => {
                    const user = interaction.guild.members.cache.get(id);
                    const userName = user ? user.displayName : 'Unknown';
                    const role = index === 0 ? '👑 Leader' : '⚔️ Member';
                    return `${role} **${userName}**`;
                }).join('\n'), 
                inline: false 
            }
        )
        .setFooter({ 
            text: `Session #${session.id.slice(-6)} • Have an amazing game!`,
            iconURL: interaction.guild.members.cache.get(session.creator)?.displayAvatarURL() || null
        })
        .setTimestamp();

    try {
        // Update the original message with finalized status
        await interaction.message.edit({ embeds: [embed], components: [] });
        console.log(`🎆 LFG Session ${session.id} finalized successfully - ${session.game} match ready!`);
    } catch (error) {
        console.error('Error updating finalized session message:', error);
    }
}

function parseDuration(durationStr) {
    const match = durationStr.match(/(\d+)([smhd])/);
    if (!match) return 60 * 60 * 1000; // Default 1 hour

    const [, amount, unit] = match;
    const num = parseInt(amount);

    switch (unit) {
        case 's': return num * 1000;
        case 'm': return num * 60 * 1000;
        case 'h': return num * 60 * 60 * 1000;
        case 'd': return num * 24 * 60 * 60 * 1000;
        default: return 60 * 60 * 1000;
    }
}

async function handleHelpCommand(interaction) {
    const embed = new EmbedBuilder()
        .setColor(0x00d4ff)
        .setTitle('🎆 **Party Up! Premium LFG Service** 🎆')
        .setDescription('🎮 **The ultimate gaming companion for elite squad formation**\n🏆 Professional-grade team building with premium features')
        .addFields(
            {
                name: '⚡ **Premium LFG Commands**',
                value: '`/lfg <game> <gamemode> <players> [info]`\n🎆 **Elite Squad Formation** - Create premium gaming sessions\n🔒 **Auto Private Channels** - Secure voice communication\n⚡ **Instant Matching** - Advanced confirmation system\n🏆 **Professional Interface** - Premium gaming experience',
                inline: false
            },
            {
                name: '🛠️ **Administrative Commands** (Staff Only)',
                value: '`/setchannel <channel>` - Configure LFG-exclusive zones\n`/embed <title> <description> [color]` - Professional announcements\n`/mod <action> <user> [reason] [duration]` - Advanced moderation\n`/endlfg` - Terminate active LFG sessions',
                inline: false
            },
            {
                name: '🎮 **Elite Game Library**',
                value: '🔫 **Valorant** • 🏰 **Fortnite** • ⚔️ **Brawlhalla** • 🏆 **The Finals**\n📺 **Roblox** • 🧡 **Minecraft** • ⚡ **Marvel Rivals** • ⚽ **Rocket League**\n🎆 **Apex Legends** • 🚁 **Call of Duty** • 🤖 **Overwatch** • 🕶️ **Among Us**',
                inline: false
            },
            {
                name: '🌟 **Premium Features**',
                value: '💸 **Private Voice Channels** - Crystal clear team communication\n💸 **Auto-Cleanup System** - Smart channel management\n💸 **Elite Confirmation** - 2-minute availability system\n💸 **Secure Access Control** - Role-based permissions\n💸 **Game Categories** - Organized by franchise\n💸 **Real-time Status** - Live session tracking',
                inline: false
            },
            {
                name: '🏆 **Professional Workflow**',
                value: '1️⃣ **Create** - Launch premium LFG with `/lfg`\n2️⃣ **Recruit** - Elite players join via smart buttons\n3️⃣ **Confirm** - 2-minute availability confirmation\n4️⃣ **Connect** - Auto-access to private voice channel\n5️⃣ **Dominate** - Professional team coordination',
                inline: false
            },
            {
                name: '🔒 **Moderation Arsenal**',
                value: '• **kick** - Immediate server removal with LFG cleanup\n• **ban** - Permanent removal with message purging\n• **mute** - Precision timeouts (10m, 1h, 1d formats)\n• **unmute** - Instant timeout removal\n• **Auto-Cleanup** - Removes users from all active sessions',
                inline: false
            }
        )
        .setFooter({ 
            text: 'Party Up! Premium LFG Service • Professional Gaming Solutions • Contact staff for enterprise features',
            iconURL: null
        })
        .setThumbnail(null)
        .setTimestamp();

    await interaction.reply({ embeds: [embed], flags: 64 });
}

// Helper functions for enhanced LFG experience
function createProgressBar(current, total, length = 12) {
    const filled = Math.round((current / total) * length);
    const empty = length - filled;
    const filledBar = '🟩'.repeat(filled);
    const emptyBar = '⬜'.repeat(empty);
    return `${filledBar}${emptyBar}`;
}

function getTimeAgo(timestamp) {
    const now = Date.now();
    const diff = now - timestamp;
    const minutes = Math.floor(diff / 60000);

    if (minutes < 1) return 'Just now';
    if (minutes < 60) return `${minutes}m ago`;

    const hours = Math.floor(minutes / 60);
    return `${hours}h ago`;
}

function getExpiryTime(createdAt) {
    const expiry = new Date(createdAt + (20 * 60 * 1000)); // 20 minutes from creation
    const remainingMs = expiry - Date.now();
    const remainingMinutes = Math.max(0, Math.ceil(remainingMs / 60000));

    if (remainingMinutes === 0) {
        return '⏰ **EXPIRED**';
    } else if (remainingMinutes <= 5) {
        return `⚡ **${remainingMinutes}m remaining** (URGENT!)`;
    } else if (remainingMinutes <= 10) {
        return `⏳ **${remainingMinutes}m remaining**`;
    } else {
        return `🕐 **${remainingMinutes}m remaining**`;
    }
}

function getDetailedExpiryTime(createdAt) {
    const expiry = new Date(createdAt + (20 * 60 * 1000));
    const remainingMs = expiry - Date.now();
    const remainingMinutes = Math.max(0, Math.ceil(remainingMs / 60000));

    if (remainingMinutes === 0) {
        return {
            text: '⏰ **EXPIRED**',
            urgent: true,
            color: 0xff0000
        };
    } else if (remainingMinutes <= 5) {
        return {
            text: `⚡ **${remainingMinutes} minutes left** - Join now!`,
            urgent: true,
            color: 0xff6b00
        };
    } else if (remainingMinutes <= 10) {
        return {
            text: `⏳ **${remainingMinutes} minutes remaining**`,
            urgent: false,
            color: 0xffaa00
        };
    } else {
        return {
            text: `🕐 **${remainingMinutes} minutes remaining**`,
            urgent: false,
            color: 0x00d4ff
        };
    }
}

function getGameEmoji(gameName) {
    const gameEmojis = {
        'Valorant': '🔫',
        'Fortnite': '🏗️',
        'Apex Legends': '🎆',
        'Call of Duty': '🚁',
        'Overwatch': '🤖',
        'Rocket League': '⚽',
        'Minecraft': '🧺',
        'Roblox': '🎮',
        'Brawlhalla': '⚔️',
        'The Finals': '🏆',
        'Marvel Rivals': '⚡',
        'Among Us': '🕵️'
    };
    return gameEmojis[gameName] || '🎮';
}

// Enhanced game descriptions for premium experience
function getGameDescription(gameName) {
    const descriptions = {
        'Valorant': 'Tactical FPS • 5v5 Competitive',
        'Fortnite': 'Battle Royale • Building Mechanics', 
        'Apex Legends': 'Squad-Based BR • Hero Abilities',
        'Call of Duty': 'Military FPS • Fast-Paced Action',
        'Overwatch': 'Team-Based Hero Shooter',
        'Rocket League': 'Vehicular Soccer • High-Octane',
        'Minecraft': 'Sandbox • Creative Building',
        'Roblox': 'Platform • Multiple Game Modes',
        'Brawlhalla': '2D Fighter • Platform Combat',
        'The Finals': 'Destructible FPS • Team Strategy',
        'Marvel Rivals': 'Hero Shooter • Marvel Universe',
        'Among Us': 'Social Deduction • Teamwork'
    };
    return descriptions[gameName] || 'Elite Gaming Experience';
}

// Enhanced time display function for better UX
function getTimeAgo(timestamp) {
    const now = Date.now();
    const diff = now - timestamp;
    const minutes = Math.floor(diff / 60000);

    if (minutes < 1) {
        return 'Just now';
    } else if (minutes === 1) {
        return '1 minute ago';
    } else if (minutes < 60) {
        return `${minutes} minutes ago`;
    } else {
        const hours = Math.floor(minutes / 60);
        if (hours === 1) {
            return '1 hour ago';
        } else {
            return `${hours} hours ago`;
        }
    }
}

// Function to handle member removal from all sessions (for moderation)
async function removeMemberFromAllSessions(memberId) {
    const affectedSessions = [];

    for (const [sessionId, session] of activeSessions) {
        if (session.currentPlayers.includes(memberId) || session.confirmedPlayers.includes(memberId)) {
            affectedSessions.push(sessionId);

            // Remove from players arrays
            session.currentPlayers = session.currentPlayers.filter(id => id !== memberId);
            session.confirmedPlayers = session.confirmedPlayers.filter(id => id !== memberId);

            // Remove voice access
            try {
                const guild = client.guilds.cache.get(session.guildId);
                const voiceChannel = guild?.channels.cache.get(session.voiceChannel);
                if (voiceChannel) {
                    await voiceChannel.permissionOverwrites.delete(memberId);
                }
            } catch (error) {
                console.error('Error removing voice access during moderation:', error);
            }

            // If they were the creator, end the session
            if (session.creator === memberId) {
                try {
                    const guild = client.guilds.cache.get(session.guildId);
                    const voiceChannel = guild?.channels.cache.get(session.voiceChannel);
                    if (voiceChannel) {
                        const category = voiceChannel.parent;
                        await voiceChannel.delete();

                        // Immediately check and cleanup empty category
                        if (category && category.name.startsWith('🎮') && category.children.cache.size === 0) {
                            await category.delete();
                            console.log(`Deleted empty category: ${category.name}`);
                        }
                    }
                } catch (error) {
                    console.error('Error deleting voice channel during moderation:', error);
                }

                activeSessions.delete(sessionId);
                userCreatedSessions.delete(memberId);
                console.log(`Ended session ${sessionId} - creator was moderated`);
            } else if (session.currentPlayers.length === 0) {
                // Session became empty, clean up
                try {
                    const guild = client.guilds.cache.get(session.guildId);
                    const voiceChannel = guild?.channels.cache.get(session.voiceChannel);
                    if (voiceChannel) {
                        const category = voiceChannel.parent;
                        await voiceChannel.delete();

                        // Immediately check and cleanup empty category
                        if (category && category.name.startsWith('🎮') && category.children.cache.size === 0) {
                            await category.delete();
                            console.log(`Deleted empty category: ${category.name}`);
                        }
                    }
                } catch (error) {
                    console.error('Error deleting empty voice channel during moderation:', error);
                }

                activeSessions.delete(sessionId);
                userCreatedSessions.delete(session.creator);
                console.log(`Cleaned up empty session ${sessionId} after moderation`);
            } else {
                // Reopen session with remaining players
                await reopenLfg(session);
                console.log(`Reopened session ${sessionId} after removing moderated member`);
            }
        }
    }

    console.log(`Removed member ${memberId} from ${affectedSessions.length} LFG sessions due to moderation`);
}

async function handleLeaveLfg(interaction) {
    const sessionId = interaction.customId.replace('leave_lfg_', '');
    const session = activeSessions.get(sessionId);

    if (!session) {
        return interaction.reply({ content: '❌ This LFG session is no longer active!', flags: 64 });
    }

    if (!session.currentPlayers.includes(interaction.user.id)) {
        return interaction.reply({ content: '❌ You are not in this LFG session!', flags: 64 });
    }

    // Don't allow session creator to leave (they should use /endlfg instead)
    if (interaction.user.id === session.creator) {
        return interaction.reply({ 
            content: '❌ As the session creator, you cannot leave. Use `/endlfg` to end the entire session instead.', 
            flags: 64 
        });
    }

    try {
        // First reply to the interaction immediately to prevent timeout
        await interaction.reply({ content: '✅ You left the LFG session.', flags: 64 });

        // Remove user from session
        session.currentPlayers = session.currentPlayers.filter(id => id !== interaction.user.id);
        session.confirmedPlayers = session.confirmedPlayers.filter(id => id !== interaction.user.id);

        // Remove voice channel access
        const voiceChannel = interaction.guild.channels.cache.get(session.voiceChannel);
        if (voiceChannel) {
            const accessRevoked = await manageVoiceChannelAccess(
                voiceChannel, 
                interaction.user.id, 
                'revoke', 
                `Left LFG session #${sessionId.slice(-6)}`
            );

            // Disconnect if user is in the voice channel
            try {
                if (interaction.member.voice.channel?.id === session.voiceChannel) {
                    await interaction.member.voice.disconnect('Left LFG session');
                    console.log(`📋 Disconnected ${interaction.user.displayName} from voice channel`);
                }
            } catch (disconnectError) {
                console.warn(`⚠️ Could not disconnect user from voice:`, disconnectError.message);
            }

            if (!accessRevoked) {
                console.warn(`⚠️ Failed to revoke voice access for ${interaction.user.displayName}`);
            }
        }

        console.log(`Player ${interaction.user.displayName} left session ${sessionId}`);

        // If session becomes empty, clean it up
        if (session.currentPlayers.length === 0) {
            try {
                const voiceChannel = interaction.guild.channels.cache.get(session.voiceChannel);
                if (voiceChannel) {
                    const category = voiceChannel.parent;
                    await voiceChannel.delete();

                    // Immediately check and cleanup empty category
                    if (category && category.name.startsWith('🎮') && category.children.cache.size === 0) {
                        await category.delete();
                        console.log(`Deleted empty category: ${category.name}`);
                    }
                }
            } catch (error) {
                console.error('Error deleting voice channel:', error);
            }
            activeSessions.delete(sessionId);
            userCreatedSessions.delete(session.creator); // Clean up creator tracking

            // Remove from database as well
            try {
                await storage.deleteSession(sessionId);
                console.log(`💾 Empty session #${sessionId.slice(-6)} removed from database`);
            } catch (dbError) {
                console.error(`❌ Failed to remove empty session from database:`, dbError);
            }

            console.log(`Session ${sessionId} deleted - no players remaining`);
            return;
        }

        // Update the session embed and reopen for new joiners
        await reopenLfg(session);

    } catch (error) {
        console.error('Error in handleLeaveLfg:', error);
        // If interaction hasn't been replied to yet, send error message
        if (!interaction.replied && !interaction.deferred) {
            try {
                await interaction.reply({ 
                    content: '❌ There was an error processing your request. Please try again.', 
                    flags: 64 
                });
            } catch (replyError) {
                console.error('Error sending error reply:', replyError);
            }
        }
    }
}

async function handleEndLfgCommand(interaction) {
    const userId = interaction.user.id;

    // Find the user's active LFG session (where they are the creator)
    // First check memory (fast path)
    let userSession = Array.from(activeSessions.entries()).find(([sessionId, session]) => 
        session.creator === userId
    );

    // If not found in memory, check database (recovery path)
    if (!userSession) {
        try {
            console.log(`🔍 Session not found in memory for user ${userId}, checking database...`);
            const dbSessions = await storage.getAllActiveSessions();
            const dbUserSession = dbSessions.find(session => session.creatorId === userId);

            if (dbUserSession) {
                // Restore session to memory
                const session = {
                    id: dbUserSession.id,
                    creator: dbUserSession.creatorId,
                    guildId: dbUserSession.guildId,
                    channelId: dbUserSession.channelId,
                    messageId: dbUserSession.messageId,
                    game: dbUserSession.game,
                    gamemode: dbUserSession.gamemode,
                    playersNeeded: dbUserSession.playersNeeded,
                    info: dbUserSession.info,
                    status: dbUserSession.status,
                    currentPlayers: Array.isArray(dbUserSession.currentPlayers) ? dbUserSession.currentPlayers : [],
                    confirmedPlayers: Array.isArray(dbUserSession.confirmedPlayers) ? dbUserSession.confirmedPlayers : [],
                    voiceChannel: dbUserSession.voiceChannelId,
                    confirmationStartTime: dbUserSession.confirmationStartTime ? new Date(dbUserSession.confirmationStartTime).getTime() : null,
                    createdAt: new Date(dbUserSession.createdAt).getTime(),
                    timeoutId: null
                };

                // Restore to memory
                activeSessions.set(dbUserSession.id, session);
                userCreatedSessions.set(userId, dbUserSession.id);
                userSession = [dbUserSession.id, session];

                console.log(`✅ Restored session #${dbUserSession.id.slice(-6)} from database for user ${userId}`);
            }
        } catch (dbError) {
            console.error('❌ Error checking database for user session:', dbError);
        }
    }

    if (!userSession) {
        return interaction.reply({ 
            content: '❌ You don\'t have an active LFG session to end!', 
            flags: 64 
        });
    }

    const [sessionId, session] = userSession;

    try {
        // Clear any timeouts
        if (session.timeoutId) {
            clearTimeout(session.timeoutId);
            session.timeoutId = null;
        }

        // Get guild and channel info
        const guild = client.guilds.cache.get(session.guildId);
        const channel = guild?.channels.cache.get(session.channelId);

        // Delete voice channel
        try {
            const voiceChannel = guild?.channels.cache.get(session.voiceChannel);
            if (voiceChannel) {
                const category = voiceChannel.parent;
                await voiceChannel.delete();
                console.log(`Deleted voice channel for ended session ${sessionId}`);

                // Immediately check and cleanup empty category
                if (category && category.name.startsWith('🎮') && category.children.cache.size === 0) {
                    await category.delete();
                    console.log(`Deleted empty category: ${category.name}`);
                }
            }
        } catch (error) {
            console.error('Error deleting voice channel:', error);
        }

        // Create professional ended embed
        const endedEmbed = new EmbedBuilder()
            .setColor(0x747f8d) // Professional gray
            .setTitle('🔚 **LFG Session Terminated**')
            .setDescription(`📼 **Session closed by ${interaction.user.displayName}**\n\n🔄 **Create a new session anytime with \`/lfg\`**\n🏆 **Party Up! - Premium LFG Service**`)
            .addFields(
                {
                    name: '📊 Session Statistics',
                    value: `**Game:** ${session.game}\n**Mode:** ${session.gamemode}\n**Duration:** ${getTimeAgo(session.createdAt)}\n**Status:** Terminated by creator`,
                    inline: true
                },
                {
                    name: '🛠️ Cleanup Actions',
                    value: '• Voice channel deleted\n• Permissions revoked\n• Session data cleared\n• Resources freed',
                    inline: true
                }
            )
            .setFooter({ 
                text: `Session #${sessionId.slice(-6)} • Party Up! Premium LFG Service`,
                iconURL: null
            })
            .setTimestamp();

        // Enhanced message updating with robust error handling
        if (channel) {
            let messageUpdated = false;

            try {
                if (session.messageId && session.messageId.length > 0) {
                    try {
                        const originalMessage = await channel.messages.fetch(session.messageId);
                        await originalMessage.edit({ embeds: [endedEmbed], components: [] });
                        console.log(`✅ Successfully updated original LFG message for session #${sessionId.slice(-6)}`);
                        messageUpdated = true;
                    } catch (fetchError) {
                        if (fetchError.code === 10008) {
                            console.log(`⚠️ Message ${session.messageId} no longer exists for session #${sessionId.slice(-6)}`);
                        } else {
                            throw fetchError;
                        }
                    }
                }

                if (!messageUpdated) {
                    // Intelligent fallback: search by session ID
                    const messages = await channel.messages.fetch({ limit: 50 });
                    const originalMessage = messages.find(msg => 
                        msg.embeds.length > 0 && 
                        msg.embeds[0].footer?.text?.includes(sessionId.slice(-6))
                    );

                    if (originalMessage) {
                        await originalMessage.edit({ embeds: [endedEmbed], components: [] });
                        console.log(`✅ Updated LFG message via fallback search for session #${sessionId.slice(-6)}`);
                        messageUpdated = true;
                    }
                }
            } catch (error) {
                console.warn(`⚠️ Message update failed for session #${sessionId.slice(-6)}: ${error.message}`);

                // Advanced recovery: comprehensive message search
                try {
                    const messages = await channel.messages.fetch({ limit: 100 });
                    const originalMessage = messages.find(msg => 
                        msg.embeds.length > 0 && 
                        (msg.embeds[0].footer?.text?.includes(sessionId.slice(-6)) ||
                         msg.components.some(row => 
                            row.components.some(comp => comp.customId?.includes(sessionId))
                         ))
                    );

                    if (originalMessage) {
                        await originalMessage.edit({ embeds: [endedEmbed], components: [] });
                        console.log(`✅ Recovered and updated LFG message for session #${sessionId.slice(-6)}`);
                        messageUpdated = true;
                    }
                } catch (recoveryError) {
                    console.error(`❌ Failed to recover message for session #${sessionId.slice(-6)}: ${recoveryError.message}`);
                }
            }

            // Graceful fallback: no spam if update fails
            if (!messageUpdated) {
                console.log(`🔄 Message update skipped for session #${sessionId.slice(-6)} - original message may have been deleted`);
            }
        }

        // Remove session from memory AND database
        activeSessions.delete(sessionId);
        userCreatedSessions.delete(session.creator); // Clean up creator tracking

        // Ensure session is properly deleted from database
        try {
            await storage.deleteSession(sessionId);
            console.log(`💾 Session #${sessionId.slice(-6)} removed from database`);
        } catch (dbError) {
            console.error(`❌ Failed to remove session from database:`, dbError);
        }

        console.log(`Session ${sessionId} ended by creator ${interaction.user.displayName}`);

        // Add interaction timeout protection
        try {
            if (!interaction.replied && !interaction.deferred) {
                await interaction.reply({ 
                    content: '✅ Your LFG session has been ended successfully!', 
                    flags: 64 
                });
            } else {
                console.log('Interaction already handled, skipping reply');
            }
        } catch (replyError) {
            if (replyError.code === 10062) {
                console.log('Interaction expired, but session was ended successfully');
            } else {
                console.error('Error replying to endlfg interaction:', replyError);
            }
        }

    } catch (error) {
        console.error('Error ending LFG session:', error);
        try {
            if (!interaction.replied && !interaction.deferred) {
                await interaction.reply({ 
                    content: '❌ There was an error ending your LFG session. Please try again.', 
                    flags: 64 
                });
            }
        } catch (replyError) {
            console.error('Error replying to endlfg interaction:', replyError);
        }
    }
}

// Simple HTTP server for Render health checks
const http = require('http');
const port = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ 
        status: 'Bot is running!', 
        uptime: process.uptime(),
        timestamp: new Date().toISOString()
    }));
});

server.listen(port, () => {
    console.log(`Health check server running on port ${port}`);
});

// Add error handlers for Discord client
client.on('error', (error) => {
    console.error('Discord client error:', error);
});

client.on('shardError', (error, shardId) => {
    console.error(`Shard ${shardId} error:`, error);
});

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('Received SIGINT, shutting down gracefully...');
    client.destroy();
    server.close();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('Received SIGTERM, shutting down gracefully...');
    client.destroy();
    server.close();
    process.exit(0);
});

// Enhanced global error handling with professional logging
process.on('unhandledRejection', (error) => {
    console.error('🚨 Unhandled promise rejection detected:', error.message);
    console.error('📍 Stack trace:', error.stack);
});

process.on('uncaughtException', (error) => {
    console.error('💥 Critical uncaught exception:', error.message);
    console.error('📍 Stack trace:', error.stack);
    console.log('🔄 Attempting graceful shutdown...');

    // Attempt to save any active sessions before exit
    if (activeSessions.size > 0) {
        console.log(`💾 Attempting to preserve ${activeSessions.size} active sessions...`);
        saveSessionData();
    }

    setTimeout(() => {
        process.exit(1);
    }, 1000);
});

// Initialize bot with proper error handling
async function startBot() {
    try {
        // Ensure database is ready before starting Discord client
        await ensureDatabaseTables();

        // Start Discord client
        await client.login(process.env.DISCORD_TOKEN);
        console.log('🎉 Bot initialization completed successfully!');

    } catch (error) {
        console.error('❌ Failed to start bot:', error);
        console.error('🔄 Shutting down...');
        process.exit(1);
    }
}

startBot();