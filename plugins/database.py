import asyncio
from tinydb import TinyDB, Query, where
from config import DB_FILE

class Database:
    
    def __init__(self, db_file):
        # Use synchronous TinyDB and wrap methods with asyncio.to_thread
        self.db = TinyDB(db_file)
        self.users_col = self.db.table('users')
        self.channels_col = self.db.table('channels')
        self.User = Query()
        self.Channel = Query()

    # Helper function to run synchronous TinyDB operations in a thread
    async def _run_sync(self, func, *args, **kwargs):
        # We use asyncio.to_thread to run synchronous TinyDB operations
        # in a separate thread, preventing blocking the main event loop.
        return await asyncio.to_thread(func, *args, **kwargs)

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            session = None,
        )
    
    def new_channel(self, user_id, channel_id, base_username, interval):
        return dict(
            user_id = user_id,
            channel_id = channel_id,
            base_username = base_username,
            interval = interval,
            is_active = True,
            last_changed = None,
        )
    
    async def add_user(self, id, name):
        user = self.new_user(id, name)
        await self._run_sync(self.users_col.insert, user)
    
    async def is_user_exist(self, id):
        user = await self._run_sync(self.users_col.get, self.User.id == int(id))
        return bool(user)
    
    async def total_users_count(self):
        # We use len(table) for synchronous count in TinyDB
        return await self._run_sync(len, self.users_col)

    async def get_all_users(self):
        return await self._run_sync(self.users_col.all)

    async def delete_user(self, user_id):
        await self._run_sync(self.users_col.remove, self.User.id == int(user_id))

    async def set_session(self, id, session):
        await self._run_sync(self.users_col.update, {'session': session}, self.User.id == int(id))

    async def get_session(self, id):
        user = await self._run_sync(self.users_col.get, self.User.id == int(id))
        return user.get('session') if user else None

    async def add_channel(self, user_id, channel_id, base_username, interval):
        channel = self.new_channel(user_id, channel_id, base_username, interval)
        await self._run_sync(self.channels_col.insert, channel)

    async def get_user_channels(self, user_id):
        return await self._run_sync(self.channels_col.search, (self.Channel.user_id == int(user_id)) & (self.Channel.is_active == True))

    async def get_all_active_channels(self):
        return await self._run_sync(self.channels_col.search, self.Channel.is_active == True)

    async def stop_channel(self, channel_id):
        await self._run_sync(self.channels_col.update, {'is_active': False}, self.Channel.channel_id == int(channel_id))

    async def resume_channel(self, channel_id):
        await self._run_sync(self.channels_col.update, {'is_active': True}, self.Channel.channel_id == int(channel_id))

    async def delete_channel(self, channel_id):
        await self._run_sync(self.channels_col.remove, self.Channel.channel_id == int(channel_id))

    async def update_last_changed(self, channel_id, timestamp):
        await self._run_sync(self.channels_col.update, {'last_changed': timestamp}, self.Channel.channel_id == int(channel_id))

    async def get_channel(self, channel_id):
        return await self._run_sync(self.channels_col.get, self.Channel.channel_id == int(channel_id))

db = Database(DB_FILE)
