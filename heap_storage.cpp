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

bool test_heap_storage() {return true;}

using u16 = u_int16_t;
using u32 = u_int32_t;

// Begin Slotted Page functions

SlottedPage::SlottedPage(Dbt& block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
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

Dbt* SlottedPage::get(RecordID record_id) {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    if (!loc) return nullptr; // Tombstone
    return new Dbt(this->address(loc), size);
}

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

void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    this->put_header(record_id);
    this->slide(loc, loc + size);
}

RecordIDs* SlottedPage::ids(void) {
    RecordIDs* record_ids = new RecordIDs();
    for (RecordID record_id = 0; record_id < this->num_records; record_id++) {
        u16 size, loc;
        this->get_header(size, loc, record_id);
        if (loc) 
            record_ids->push_back(record_id);
    }
    return record_ids;
}

void SlottedPage::get_header(u16& size, u16& loc, RecordID id){
    size = get_n(4*id);
    loc = get_n(4*id+2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

bool SlottedPage::has_room(u16 size){
    u16 available = this->end_free - (this->num_records+1)*4;
    return size + 4 <= available;
}

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

void HeapFile::create(void) {
    u32 flags = DB_CREATE | DB_EXCL;
    this->db_open(flags);
    SlottedPage* block = this->get_new();
    this->put(block);
}

void HeapFile::drop(void) {
    this->close();
    const char** pHome = nullptr;
    int status = _DB_ENV->get_home(pHome);
    if (status)
        throw std::logic_error("could not remove DB file");
    std::string dbfilepath = std::string(*pHome) + "/" + this->dbfilename;
    status = std::remove(dbfilepath.c_str());
    if (status)
        throw std::logic_error("could not remove DB file");
}

void HeapFile::open(void) {
    this->db_open();
}

void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

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

SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id)), block;
    this->db.get(NULL, &key, &block, 0);
    return new SlottedPage(block, block_id);
}

void HeapFile::put(DbBlock* block) {
    BlockID block_id = block->get_block_id();
    Dbt* data = block->get_block();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(NULL, &key, data, 0);
}

BlockIDs* HeapFile::block_ids() {
    BlockIDs* block_ids = new BlockIDs();
    for (BlockID block_id = 1; block_id <= this->last; block_id++)
        block_ids->push_back(block_id);
    return block_ids;
}

// End Heap File Functions

// Begin heap table Functions

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file(table_name)
{}

void HeapTable::create() {
    this->file.create();
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
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

Handles* HeapTable::select(const ValueDict* where) {
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

// End Heap Table Functions