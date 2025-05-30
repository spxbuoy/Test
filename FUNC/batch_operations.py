"""
Batch Operations Module

This module provides functions for performing batch operations
to improve efficiency and reduce API rate limiting.
"""
import asyncio
from typing import List, Dict, Any, Callable, Tuple
from FUNC.defs import error_log

async def process_in_batches(items: List[Any], 
                             process_func: Callable[[Any], Any], 
                             batch_size: int = 10,
                             delay_between_items: float = 0.2,
                             delay_between_batches: float = 2.0) -> Tuple[List[Any], List[Any]]:
    """
    Process a list of items in batches with specified delays to avoid rate limiting.
    
    Args:
        items: List of items to process
        process_func: Async function to process each item
        batch_size: Number of items to process in each batch
        delay_between_items: Delay in seconds between processing items in a batch
        delay_between_batches: Delay in seconds between batches
        
    Returns:
        Tuple containing (successful_items, failed_items)
    """
    successful = []
    failed = []
    
    # Process in batches
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        
        # Process items in this batch
        for item in batch:
            try:
                result = await process_func(item)
                successful.append((item, result))
            except Exception as e:
                await error_log(f"Batch operation error on item {item}: {str(e)}")
                failed.append((item, str(e)))
                
            # Delay between items to avoid rate limiting
            if delay_between_items > 0:
                await asyncio.sleep(delay_between_items)
        
        # Delay between batches
        if i + batch_size < len(items) and delay_between_batches > 0:
            await asyncio.sleep(delay_between_batches)
    
    return successful, failed

async def batch_leave_channels(client, channel_ids: List[Any]) -> Tuple[List[Any], List[Any]]:
    """
    Leave multiple channels in batches to avoid rate limiting.
    
    Args:
        client: Pyrogram client
        channel_ids: List of channel IDs to leave
        
    Returns:
        Tuple containing (successfully_left, failed_to_leave)
    """
    from FUNC.scraperfunc import leave_channel
    
    async def process_channel(channel_id):
        return await leave_channel(client, channel_id)
    
    # Process channels in batches of 5 with appropriate delays
    successful, failed = await process_in_batches(
        items=channel_ids,
        process_func=process_channel,
        batch_size=5,
        delay_between_items=1.0,
        delay_between_batches=5.0
    )
    
    # Update database for successful and failed channels
    from CONFIG_DB import CHANNELS_DB
    
    for channel_id, result in successful:
        if result:
            print(f"Successfully left channel {channel_id} in batch operation")
    
    return successful, failed

async def batch_scrape_messages(client, channel_id, limit: int, filter_func=None):
    """
    Fetch and process messages in batches for more efficient scraping.
    
    Args:
        client: Pyrogram client
        channel_id: Channel ID to scrape
        limit: Maximum number of messages to scrape
        filter_func: Optional function to filter messages (returns True for messages to keep)
        
    Returns:
        List of processed messages
    """
    from pyrogram.types import Message
    
    # Constants for batch processing
    BATCH_SIZE = 100  # Number of messages to fetch in each batch
    
    results = []
    offset_id = 0
    remaining = limit
    
    while remaining > 0:
        # Calculate how many messages to fetch in this batch
        current_limit = min(BATCH_SIZE, remaining)
        
        # Fetch batch of messages
        messages = await client.get_messages(
            chat_id=channel_id,
            limit=current_limit,
            offset_id=offset_id
        )
        
        # Break if no messages were returned
        if not messages:
            break
            
        # Process messages
        for msg in messages:
            # Apply filter if provided
            if filter_func and not filter_func(msg):
                continue
                
            # Add to results
            results.append(msg)
            
            # Update offset_id for pagination
            if msg.id < offset_id or offset_id == 0:
                offset_id = msg.id
        
        # Update remaining count
        remaining -= current_limit
        
        # Small delay to avoid rate limiting
        await asyncio.sleep(0.5)
    
    return results