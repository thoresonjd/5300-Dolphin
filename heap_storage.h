/**
 * @file heap_storage.h - Implementation of storage_engine with a heap file structure.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#pragma once

#include "db_cxx.h"
#include "storage_engine.h"

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 * Manage a database block that contains several records.
 * Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.
 *
 * Record id are handed out sequentially starting with 1 as records are added with add().
 * Each record has a header which is a fixed offset from the beginning of the block:
 *     Bytes 0x00 - Ox01: number of records
 *     Bytes 0x02 - 0x03: offset to end of free space
 *     Bytes 0x04 - 0x05: size of record 1
 *     Bytes 0x06 - 0x07: offset to record 1
 *     etc.
 */
class SlottedPage : public DbBlock {
public:
    SlottedPage(Dbt& block, BlockID block_id, bool is_new = false);

    // Big 5 - we only need the destructor, copy-ctor, move-ctor, and op= are unnecessary
    // but we delete them explicitly just to make sure we don't use them accidentally
    virtual ~SlottedPage() {}

    SlottedPage(const SlottedPage& other) = delete;

    SlottedPage(SlottedPage&& temp) = delete;

    SlottedPage& operator=(const SlottedPage& other) = delete;

    SlottedPage& operator=(SlottedPage& temp) = delete;

    /**
     * Adds a new record to a slotted page
     * @param data The data of the record
     * @return The record ID of the record
     */
    virtual RecordID add(const Dbt* data);

    /**
     * Retrieves a record from a slotted page
     * @param record_id The ID of the record to retrieve
     * @return The data of the record (location and size)
     */
    virtual Dbt* get(RecordID record_id);

    /**
     * Puts a new record in the place of an existing record in a slotted page
     * @param record_id The ID of the record to replace
     * @param data The data to replace the existing record with
     */
    virtual void put(RecordID record_id, const Dbt& data);

    /**
     * Removes a record from a slotted page
     * @param record_id The ID of the record to remove 
     */
    virtual void del(RecordID record_id);

    /**
     * Retrieves the IDs of all records in a slotted page
     */
    virtual RecordIDs* ids(void);

protected:
    u_int16_t num_records;
    u_int16_t end_free;

    /**
     * Retrieves the header (size and location) of the record within a slotted page
     * @param size The size of the record
     * @param loc The location (offset) of the record
     * @param id The ID of the record
     */
    virtual void get_header(u_int16_t& size, u_int16_t& loc, RecordID id = 0);

    /**
     * Store the size and offset for given ID. For ID = 0, store the block header.
     * @param id The ID of a record
     * @param size The size of the record
     * @param loc The location (offset) of the record
     */
    virtual void put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0);

    /**
     * Checks the slotted page if there is enough free memory to add a new record
     * @param size The size of the new record
     * @return True if the record can fit into the slotted page, false otherwise
     */
    virtual bool has_room(u_int16_t size);

    /**
     * If start < end, then remove data from offset start up to but not including
     * offset end by sliding data that is to the left of start to the right. If
     * start > end, then make room for extra data from end to start by sliding data
     * that is to the left of start to the left. Also fix up any record headers
     * whose data has slid. Assumes there is enough room if it is a left shift 
     * (end < start).
     * @param start The begining of section of memory in a slotted page
     * @param end The end of section of memory in a slotted page
     */
    virtual void slide(u_int16_t start, u_int16_t end);

    /** 
     * Get 2-byte integer at given offset in block.
     */ 
    virtual u_int16_t get_n(u_int16_t offset);

    /** 
     * Put a 2-byte integer at given offset in block.
     */
    virtual void put_n(u_int16_t offset, u_int16_t n);

    /** 
     * Make a void* pointer for a given offset into the data block.
     */
    virtual void *address(u_int16_t offset);
};

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one
 * of our database blocks for each Berkeley DB record in the RecNo file.
 * In this way we are using Berkeley DB for buffer management and file
 * management. Uses SlottedPage for storing records within blocks.
 */
class HeapFile : public DbFile {
public:
    HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {}

    virtual ~HeapFile() {}

    HeapFile(const HeapFile& other) = delete;

    HeapFile(HeapFile&& temp) = delete;

    HeapFile& operator=(const HeapFile& other) = delete;

    HeapFile& operator=(HeapFile&& temp) = delete;

    /**
     * Create physical database file
     */
    virtual void create(void);

    /**
     * Remove physical database file
     */
    virtual void drop(void);

    /**
     * Open the database file
     */
    virtual void open(void);

    /**
     * Close the database file
     */
    virtual void close(void);

    /**
     * Allocate a new block for the database file.
     * Returns the new empty DbBlock that is managing the records in this block and its block id.
     */
    virtual SlottedPage* get_new(void);

    /**
     * Retrieves a block from the database file
     * @param block_id The id of the block to retrieve
     * @return A slotted page, the data of the block requested
     */
    virtual SlottedPage* get(BlockID block_id);

    /**
     * Writes a block to the database file
     * @param block The block to write to the database file
     */
    virtual void put(DbBlock* block);

    /**
     *  Retrieves all block IDs of blocks within the database file
     */
    virtual BlockIDs* block_ids();

    /**
     * Retrieves the last block ID within the file
     */
    virtual u_int32_t get_last_block_id() { return last; }

protected:
    std::string dbfilename;
    u_int32_t last;
    bool closed;
    Db db;

    /**
     * Open the Berkeley DB database file
     * @param flags Flags to provide the Berkeley DB database file
     */ 
    virtual void db_open(uint flags = 0);
};

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */
class HeapTable : public DbRelation {
public:
    HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes);

    virtual ~HeapTable() {}

    HeapTable(const HeapTable& other) = delete;

    HeapTable(HeapTable&& temp) = delete;

    HeapTable& operator=(const HeapTable& other) = delete;

    HeapTable& operator=(HeapTable&& temp) = delete;

    /**
     * Creates the HeapTable relation
     */
    virtual void create();

    /** 
     * Creates the HeapTable relation if it doesn't already exist
     */ 
    virtual void create_if_not_exists();

    /**
     * Drops the HeapTable relation
     */
    virtual void drop();

    /**
     * Opens the HeapTable relation
     */
    virtual void open();

    /**
     * Closes the HeapTable relation
     */
    virtual void close();

    /**
     * Inserts a data tuple into the table
     * @param row The data tuple to insert
     * @return A handle locating the block ID and record ID of the inserted tuple
     */
    virtual Handle insert(const ValueDict* row);

    /**
     * Updates a record to a database
     * @param handle The location (block ID, record ID) of the record
     * @param new_values The new fields to replace the existing fields with
     */
    virtual void update(const Handle handle, const ValueDict* new_values);

    /**
     * Deletes a row from the table using the given handle for the row
     * @param handle The handle for the row being deleted
     */
    virtual void del(const Handle handle);

    /**
     * Select all data tuples (rows) from the table
     */
    virtual Handles* select();

    /**
     * Selects data tuples (rows) from the table matching given predicates
     * @param where The where-clause predicates
     * @return Handles locating the block IDs and record IDs of the matching rows
     */
    virtual Handles* select(const ValueDict* where);

    /**
     * Return a sequence of all values for handle (SELECT *).
     * @param handle Location of row to get values from
     * @returns Dictionary of values from row (keyed by all column names)
     */
    virtual ValueDict* project(Handle handle);

    /**
     * Return a sequence of values for handle given by column_names
     * @param handle Location of row to get values from
     * @param column_names List of column names to project
     * @returns Dictionary of values from row (keyed by column_names)
     */
    virtual ValueDict* project(Handle handle, const ColumnNames* column_names);

protected:
    HeapFile file;

    /**
     * Checks if a row is valid to the table
     * @param row The data tuple to validate
     * @return The fully validated data tuple
     */
    virtual ValueDict* validate(const ValueDict* row);

    /**
     * Writes a data tuple to the database file
     * @param row The data tuple to add
     * @return A handle locating the block ID and record ID of the written tuple
     */
    virtual Handle append(const ValueDict* row);

    /**
     * Return the bits to go into the file. Caller responsible for freeing the
     * returned Dbt and its enclosed ret->get_data().
     */
    virtual Dbt* marshal(const ValueDict* row);
    
    /**
     * Converts data bytes into concrete types
     */
    virtual ValueDict* unmarshal(Dbt* data);
};

/**
 * Heap storage test function. Returns true if all tests pass.
 */
bool test_heap_storage();

