#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
 
typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef enum {EXECUTE_SUCCESS, EXECUTE_FAIL, EXECUTE_TABLE_FULL} ExecuteResult;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS, 
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {STATEMENT_INSERT, STATEMENT_SELECT} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

#define ROWS_PER_PAGE 14
#define TABLE_MAX_PAGES 100
#define TABLE_MAX_ROWS (ROWS_PER_PAGE * TABLE_MAX_PAGES)

typedef struct {
    unsigned id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct {
    Row *rows[ROWS_PER_PAGE];
} Page;

typedef struct {
    unsigned int num_rows;
    Page *pages[TABLE_MAX_PAGES];
} Table;


typedef struct {
    StatementType type;
    Row row_to_insert; // only used by insert statement
} Statement;


void print_row (Row* row);

void move_row(Row* source, Row* destination);

Row* row_slot(Table* table, unsigned int row_num);

Table* new_table();

void free_table(Table* table);

Statement* new_statement();

InputBuffer* new_input_buffer();

void print_prompt();

ssize_t getline(char **restrict lineptr, size_t *restrict n, FILE *restrict stream);

void read_input(InputBuffer* input_buffer);
void clear_input_buffer(InputBuffer* input_buffer);
void close_input_buffer(InputBuffer* input_buffer);

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table *table);

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement);

ExecuteResult execute_statement(Statement* statement, Table *table);

int main()
{
    InputBuffer* input_buffer = new_input_buffer();
    Table* table = new_table();
    while (true){
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.'){
            switch (do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;

        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s.'\n", input_buffer->buffer);
                continue;
        }


        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("%s\n", input_buffer->buffer);
                for (unsigned i = 0; i < table->num_rows; i++) {
                    print_row(row_slot(table, i));
                }
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
                break;
            case (EXECUTE_FAIL):
                printf("Failed To Execute.\n");
                break;

        }
        clear_input_buffer(input_buffer);
    }
}

Page* new_page() {
    Page* page = (Page*)malloc(sizeof(Page));
    for (unsigned i = 0; i < ROWS_PER_PAGE; i++) {
        page->rows[i] = malloc(sizeof(Row));
    }
    return page;
}

void free_page(Page* page) {
    for (int i = 0; i < ROWS_PER_PAGE; i++) {
        free(page->rows[i]);
    }
    free(page);
}

Table* new_table() {
    Table* table = malloc(sizeof(Table));
    table->num_rows = 0;
    for (unsigned i = 0; i < TABLE_MAX_PAGES; i++) {
        table->pages[i] = NULL;
    }
    return table;
}

void free_table(Table* table) {
    for (int i = 0; table->pages[i]; i++) {
        free_page(table->pages[i]);
    }
    free(table);
}

Statement* new_statement() {
    Statement* statement = malloc(sizeof(Statement));

    return statement;

}

void print_row (Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void move_row(Row* source, Row* destination) {
   *destination = *source;
}


Row* row_slot(Table* table, unsigned num_row) {
    unsigned page_num = num_row / ROWS_PER_PAGE;
    Page *page = table->pages[page_num];
    if (page == NULL) {
        page = table->pages[page_num] = new_page();
    }

    unsigned row_num = num_row % ROWS_PER_PAGE;

    return page->rows[row_num];

}

InputBuffer* new_input_buffer()
{
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

void print_prompt()
{
    printf("db > ");
}

ssize_t getline(char **restrict lineptr, size_t *restrict n, FILE *restrict stream) {

    register char c;
    register char *cs = NULL;
    register size_t length = 0;
    while ((c = (char) getc(stream)) != EOF) {
        cs = (char *)realloc(cs, ++length + 1);
        if ((*(cs + length - 1) = c) == '\n') {
            *(cs + length) = '\0';
            break;
        }
    }

    // return the allocated memory if lineptr is null
    if ((*lineptr) == NULL) {
        *lineptr = cs;
    } 
    else {
        // check if enough memory is allocated 
        if ((length + 1) < *n) {
            *lineptr = (char *)realloc(*lineptr, length + 1);
        }
        memcpy(*lineptr, cs, length);
        free(cs);
    }
    return (ssize_t)(*n = strlen(*lineptr));
}
void read_input(InputBuffer* input_buffer)
{
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0){
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}
void clear_input_buffer(InputBuffer* input_buffer)
{
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
}

void close_input_buffer(InputBuffer* input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table *table)
{
    if(strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        free_table(table);
        exit(EXIT_SUCCESS);
    }
    else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement)
{
    if (strncmp(input_buffer->buffer, "insert", 6) == 0){
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(input_buffer->buffer, "insert %u %s %s", &(statement->row_to_insert.id), statement->row_to_insert.username, statement->row_to_insert.email);

        if (args_assigned < 3) {
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement, Table *table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    move_row(&(statement->row_to_insert), row_slot(table, table->num_rows));
    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Table* table) {
    for (unsigned i = 0; i < table->num_rows; i++) {
        print_row(row_slot(table, i));
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);

        case (STATEMENT_SELECT):
            return execute_select(table);
    }
    return EXECUTE_FAIL;
}