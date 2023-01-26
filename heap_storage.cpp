/**
 * @file heap_storage.cpp - Implementation of the heap storage storage engine.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @authors Justin Thoreson & Mason Adsero
 * @see "Seattle University, CPSC5300, Winter 2023"
 */

#include "heap_storage.h"
#include <cstring>
#include "db_cxx.h"

using u16 = u_int16_t;
using u32 = u_int32_t;

// Begin Slotted Page functions

// Ctor for slotted page
SlottedPage::SlottedPage(Dbt& block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

/**
 * Adds a new record to a slotted page
 * @param data The data of the record
 * @return The record ID of the record
 */
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

/**
 * Retrieves a record from a slotted page
 * @param record_id The ID of the record to retrieve
 * @return The data of the record (location and size)
 */
Dbt* SlottedPage::get(RecordID record_id) {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    if (!loc) return nullptr; // Tombstone
    return new Dbt(this->address(loc), size);
}

/**
 * Puts a new record in the place of an existing record in a slotted page
 * @param record_id The ID of the record to replace
 * @param data The data to replace the existing record with
 */
void SlottedPage::put(RecordID record_id, const Dbt& data) {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    u16 new_size = sizeof(data);
    if (new_size > size) {
        u16 extra = new_size - size;
        if (!this->has_room(extra))
            throw DbBlockNoRoomError("not enough room in block");
        this->slide(loc + new_size, loc + size);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        this->slide(loc + new_size, loc + size);
    }
    this->get_header(size, loc, record_id);
    this->put_header(record_id, new_size, loc);
}

/**
 * Removes a record from a slotted page
 * @param record_id The ID of the record to remove 
 */
void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    this->put_header(record_id);
    this->slide(loc, loc + size);
}

// Retrieves the IDs of all records in a slotted page
RecordIDs* SlottedPage::ids(void) {
    RecordIDs* record_ids = new RecordIDs();
    for (RecordID record_id = 1; record_id <= this->num_records; record_id++) {
        u16 size, loc;
        this->get_header(size, loc, record_id);
        if (loc) 
            record_ids->push_back(record_id);
    }
    return record_ids;
}

/**
 * Retrieves the header (size and location) of the record within a slotted page
 * @param size The size of the record
 * @param loc The location (offset) of the record
 * @param id The ID of the record
 */
void SlottedPage::get_header(u16& size, u16& loc, RecordID id){
    size = get_n(4*id);
    loc = get_n(4*id+2);
}

/**
 * Store the size and offset for given ID. For ID = 0, store the block header.
 * @param id The ID of a record
 * @param size The size of the record
 * @param loc The location (offset) of the record
 */ 
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

/**
 * Checks the slotted page if there is enough free memory to add a new record
 * @param size The size of the new record
 * @return True if the record can fit into the slotted page, false otherwise
 */
bool SlottedPage::has_room(u16 size){
    u16 available = this->end_free - (this->num_records+1)*4;
    return size + 4 <= available;
}

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
void SlottedPage::slide(u16 start, u16 end) {
    u16 shift = end - start;
    if (!shift) return;
    
    // Slide data
    void* old_loc = this->address(this->end_free + 1);
    void* new_loc = this->address(this->end_free + shift + 1);
    u16 bytes = start - (this->end_free + 1);
    memmove(new_loc, old_loc, bytes);

    // Fixup headers
    RecordIDs* record_ids = this->ids();
    for (RecordID record_id : *record_ids) {
        u16 size, loc;
        this->get_header(size, loc, record_id);
        if (loc <= start) {
            loc += shift;
            this->put_header(record_id, size, loc);
        }
    }
    delete record_ids;
    this->end_free += shift;
    this->put_header(); // Update main block header
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// End Slotted Page Functions

// Begin Heap File Functions

// Create physical database file
void HeapFile::create(void) {
    u32 flags = DB_CREATE; // FIXME: figure out if DB_EXCL DB_TRUNCATE are necessary
    this->db_open(flags);
    SlottedPage* block = this->get_new();
    this->put(block);
    delete block;
}

// Remove physical database file
void HeapFile::drop(void) {
    this->close();
    const char** pHome = new const char*[1024];
    int status = _DB_ENV->get_home(pHome);
    if (status)
        throw std::logic_error("could not remove DB file");
    std::string dbfilepath = std::string(*pHome) + "/" + this->dbfilename;
    status = std::remove(dbfilepath.c_str());
    if (status)
        throw std::logic_error("could not remove DB file");
}

// Open the database file
void HeapFile::open(void) {
    this->db_open();
}

// Close the database file
void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

/**
 * Open the Berkeley DB database file
 * @param flags Flags to provide the Berkeley DB database file
 */
void HeapFile::db_open(uint flags) {
    if (!this->closed) return;
    this->db.set_message_stream(_DB_ENV->get_message_stream());
    this->db.set_error_stream(_DB_ENV->get_error_stream());
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->dbfilename = this->name + ".db";
    int status = this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0);
    if (status)
        this->close();
    else
        this->closed = false;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

/**
 * Retrieves a block from the database file
 * @param block_id The id of the block to retrieve
 * @return A slotted page, the data of the block requested
 */
SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id)), block;
    this->db.get(NULL, &key, &block, 0);
    return new SlottedPage(block, block_id);
}

/**
 * Writes a block to the database file
 * @param block The block to write to the database file
 */
void HeapFile::put(DbBlock* block) {
    BlockID block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    Dbt* data = block->get_block();
    this->db.put(NULL, &key, data, 0);
}

// Retrieves all block IDs of blocks within the database file
BlockIDs* HeapFile::block_ids() {
    BlockIDs* block_ids = new BlockIDs();
    for (BlockID block_id = 1; block_id <= this->last; block_id++)
        block_ids->push_back(block_id);
    return block_ids;
}

// End Heap File Functions

// Begin heap table Functions

// Ctor for HeapTable
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file(table_name)
{}

// Creates the HeapTable relation
void HeapTable::create() {
    this->file.create();
}

// Creates the HeapTable relation if it doesn't already exist
void HeapTable::create_if_not_exists() {
    try {
        this->create();
    } catch (DbRelationError& e) {
        this->open();
    }
}

// Drops the HeapTable relation
void HeapTable::drop() {
    try {
        this->file.drop();
    } catch (std::logic_error& e) {
        std::cerr << e.what() << std::endl;
    }
}

// Opens the HeapTable relation
void HeapTable::open() {
    this->file.open();
}

// Closes the HeapTable relation
void HeapTable::close() {
    this->file.close();
}

/**
 * Inserts a data tuple into the table
 * @param row The data tuple to insert
 * @return A handle locating the block ID and record ID of the inserted tuple
 */
Handle HeapTable::insert(const ValueDict* row) {
    this->open();
    ValueDict* full_row = this->validate(row);
    Handle handle = this->append(full_row);
    delete full_row;
    return handle;
}

/**
 * Updates a record to a database
 * @param handle The location (block ID, record ID) of the record
 * @param new_values The new fields to replace the existing fields with
 */
void HeapTable::update(const Handle handle, const ValueDict* new_values) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = this->file.get(block_id);
    ValueDict* row = this->project(handle);
    for(ValueDict::const_iterator it = new_values->begin(); it != new_values->end(); it++)
        (*row)[it->first] = it->second;
    ValueDict* full_row = this->validate(row);
    block->put(record_id, *this->marshal(full_row));
    this->file.put(block);
    delete block;
    return;
}

/**
 * Deletes a row from the table using the given handle for the row
 * @param handle The handle for the row being deleted
 */
void HeapTable::del(const Handle handle) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
    return;
}

// Select without where-clause
Handles* HeapTable::select() {
    return this->select(nullptr);
}

/**
 * Selects data tupels (rows) from the table matching given predicates
 * @param where The where-clause predicates
 * @return Handles locating the block IDs and record IDs of the matching rows
 */
Handles* HeapTable::select(const ValueDict* where) {
    // FIXME: ignoring where, limit, order, and group
    if (where)
        throw DbRelationError("cannot handle where clauses yet");
    
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

/**
 * Return a sequence of all values for handle (SELECT *).
 * @param handle Location of row to get values from
 * @returns Dictionary of values from row (keyed by all column names)
 */
ValueDict* HeapTable::project(Handle handle) {
    return this->project(handle, nullptr);
}

/**
 * Return a sequence of values for handle given by column_names
 * @param handle Location of row to get values from
 * @param column_names List of column names to project
 * @returns Dictionary of values from row (keyed by column_names)
 */
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = this->file.get(block_id);
    Dbt* record = block->get(record_id);
    ValueDict* row = this->unmarshal(record);
    delete block;
    delete record;
    if (column_names) {
        ValueDict* temp_row = new ValueDict();
        for (Identifier& column_name : this->column_names)
            (*temp_row)[column_name] = (*row)[column_name];
        delete row;
        row = temp_row;   
    }
    return row;
}

/**
 * Checks if a row is valid to the table
 * @param row The data tuple to validate
 * @return The fully validated data tuple
 */
ValueDict* HeapTable::validate(const ValueDict* row) {
    ValueDict* full_row = new ValueDict();
    for (Identifier& column_name : this->column_names) {
        ValueDict::const_iterator column = row->find(column_name);
        if (column == row->end())
            throw DbRelationError("missing column name");
        (*full_row)[column_name] = column->second;
    }
    return full_row;
}

/**
 * Writes a data tuple to the database file
 * @param row The data tuple to add
 * @return A handle locating the block ID and record ID of the written tuple
 */
Handle HeapTable::append(const ValueDict* row) {
    Dbt* data = this->marshal(row);
    SlottedPage* block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbBlockNoRoomError& e) {
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    delete data;
    delete block;
    return Handle(this->file.get_last_block_id(), record_id);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char* bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char* right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt* data = new Dbt(right_size_bytes, offset);
    return data;
}

// Converts data bytes into concrete types
ValueDict* HeapTable::unmarshal(Dbt* data) {
    ValueDict* row = new ValueDict();
    char* bytes = (char*)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (Identifier& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            Value value = Value(*(u32*)(bytes + offset));
            (*row)[column_name] = value;
            offset += sizeof(u32);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16*)(bytes + offset);
            offset += sizeof(u16);
            Value value(std::string(bytes + offset, size));
            (*row)[column_name] = value;
            offset += size;
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
    }
    return row;
}

// End Heap Table Functions

// test function -- returns true if all tests pass
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
    std::cout << "column names ok" << std::endl;
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    std::cout << "column attributes ok" << std::endl;
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    std::cout << "table1 construction ok" << std::endl;
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    std::cout << "table construction ok" << std::endl;
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict* result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();

    return true;
}
