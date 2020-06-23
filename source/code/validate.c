#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/debug.h"
#include "../include/queries.h"
#include "../include/free.h"
#include <time.h>


int table_has_column(table_def_t *table,char *column){
    data_column_t *tmp_ptr=table->columns;
    while(tmp_ptr){                            
        if(strcmp(tmp_ptr->object,column)==0){
            return 1;
        }
        tmp_ptr=tmp_ptr->next;
    }
    return 0;
}

/* Function: validate_create_table
 * -----------------------
 * validate a create_table structures logic
 *
 * fail if:  
 *   if the table exists in the curent global memory
 * returns: 1 for success
 *          zero or null otherwise
 */
int validate_create_table(cursor_t * cursor,table_def_t *table){
    /*
    table->column;
    table->columns;
    table->file;
    table->identifier;
    table->strict;
    */

    char *msg=0;    
    table_def_t *next=cursor->tables;
    // the source will always be available. this is caught in the parsing phase
    // the db may not be set.
    if(table->identifier->qualifier==0) {
        table->identifier->qualifier=get_current_database(cursor);
    }
    // check to see if table exists
    while(next){
        if(next->identifier) {
            if(compare_identifiers(next->identifier,table->identifier)){
                msg=safe_malloc(1000,1);
                sprintf(msg,"Table already exists %s.%s",table->identifier->qualifier,table->identifier->source);
                set_error(cursor,ERR_TABLE_ALREADY_EXISTS,msg);
                return 0;
            }
        }
        next=next->next;
    }
    // check to see if file is accessable
    if( access( table->file, F_OK) != -1 ) {
        if( access( table->file, R_OK) != -1 ) {
            if( access( table->file, W_OK) != -1 ) {
            } else {
                msg=safe_malloc(1000,1);
                sprintf(msg,"Cant write to file %s",table->file);
                set_error(cursor,ERR_FILE_WRITE_PERMISSION,msg);
                return 0;
            }
        } else {
            msg=safe_malloc(1000,1);
            sprintf(msg,"Cant read from file %s",table->file);
            set_error(cursor,ERR_FILE_READ_PERMISSION,msg);
            return 0;
        }
    } else {
        msg=safe_malloc(1000,1);      
        sprintf(msg,"Cant find file %s",table->file);
        set_error(cursor,ERR_FILE_NOT_FOUND,msg);
        return 0;
    }
    //check to see if table has columns and they are uniquely named

    if(table->columns==0) {
        msg=safe_malloc(1000,1);      
        sprintf(msg,"no columns in %s.%s",table->identifier->qualifier,table->identifier->source);
        set_error(cursor,ERR_TABLE_HAS_NO_COLUMNS,msg);
        return 1;
    }
    data_column_t *outer_tmp=table->columns;
        data_column_t *inner_tmp;
    int outer_index=0;
    int inner_index=0;
    while(outer_tmp){
        if(outer_tmp->type!=TOKEN_IDENTIFIER) {
            inner_tmp=table->columns;
            inner_index=0;
            while(inner_tmp){
                // skip itself
                if(inner_index!=outer_index) {
                    // if you find a duplicate column error out
                    if(outer_tmp->type==inner_tmp->type && strcmp(outer_tmp->object,inner_tmp->object)==0) {
                        msg=safe_malloc(1000,1);      
                        sprintf(msg,"Column must be a unique literal %s",(char*)inner_tmp->object);
                        set_error(cursor,ERR_AMBIGUOUS_COLUMN_NAME,msg);
                        return 0;
                    }
                }
                ++inner_index;
                inner_tmp=inner_tmp->next;
            }

        } else {
            msg=safe_malloc(1000,1);      
            sprintf(msg,"Column must be a unique literal");
            set_error(cursor,ERR_INVALID_COLUMN_NAME,msg);
            return 0;
        }
        outer_tmp=outer_tmp->next;
        ++outer_index;
    }


    // set most recent addition to active. append to end of list. update next and tail
    table_def_t *new_table=duplicate_table(table);
    if(cursor->tables==0)  {
        cursor->tables=new_table;
        cursor->tables->tail=new_table;
        cursor->active_table=new_table;
    } else {
        cursor->tables->tail->next=new_table;
        cursor->tables->tail=new_table;
        cursor->active_table=new_table;
    }

    return 1;
}

/* Function: validate_use
 * -----------------------
 * validate a use_t structures logic
 *
 * fail if:  
 *   never... any database name is valid.. for now.
 * returns: 1 for success
 *          zero or null otherwise
 */
int validate_use(cursor_t *cursor,use_t *use){
    if(use){
        if(!use->database){
            char *err_msg=malloc(1024);
            sprintf(err_msg,"database not specified");
            set_error(cursor,ERR_INVALID_DATABASE,err_msg);
        }
        // cleanup prior name
        if(cursor->active_database) free_string(cursor->active_database);
        // set curent name
        cursor->active_database=string_duplicate(use->database);
    }
    return 1;
}


/* Function: validate_select
 * -----------------------
 * validate a select structures logic
 * 
 * returns: nothing
 */
int validate_select(cursor_t * cursor,select_t *select){
    char *err_msg=0;
    /*
    Select rules...
    select from a list. it can be a function, identifier, or expression and can have an alias
    select all column's must be unique
    all unnamed qualifiers will be applied to whatever from/join available
    targets must exist
    if ambiguious then abort.
    group and order columns must come from select list
    .. JOB 1 determine available source (data) columns and identity conflicts within...
    */
    printf("***IN FIXUP \n");
    // TODO add ERROR OUTPUT.... TO CURSOR....
   // count columns and create an array
    data_column_t *tmp_ptr=select->columns;
    int column_length=0;
    while(tmp_ptr){
        ++column_length;
        tmp_ptr=tmp_ptr->next;
    }

    if(column_length==0) {
        err_msg=malloc(1024);
        sprintf(err_msg,"Select missing select list/columns");
        set_error(cursor,ERR_MISSING_COLUMNS,err_msg);
        return 0;
    }

    // FIXUP populate column alias if missing.
    tmp_ptr=select->columns;   
    while(tmp_ptr){
        switch(tmp_ptr->type){
            case TOKEN_STRING:        
            case TOKEN_NUMERIC:       
            case TOKEN_HEX:           
            case TOKEN_BINARY:        
            case TOKEN_REAL:          
            case TOKEN_NULL: 
                            if(tmp_ptr->alias==0) {
                                if(tmp_ptr->object) {
                                    tmp_ptr->alias=string_duplicate((char *)tmp_ptr->object);
                                }
                            }                             
                             break;
            case TOKEN_IDENTIFIER:    
                             if (tmp_ptr->alias==0) {
                                tmp_ptr->alias=string_duplicate(((identifier_t *)tmp_ptr->object)->source);
                             }
                             break;
            
            break;
            default: 
                            err_msg=malloc(1024);
                            sprintf(err_msg,"Unknown Select token in select list: %s",token_type(tmp_ptr->type));
                            set_error(cursor,ERR_SELECT_LIST_UNKNOWN_TOKEN,err_msg);
                            return 0;
        }
        tmp_ptr=tmp_ptr->next;
    }
   
    // VALIDATE UNIQUE COLUMNS
    tmp_ptr=select->columns;
    while(tmp_ptr){
        data_column_t *tmp_ptr2=select->columns;
        while(tmp_ptr2){
            if(tmp_ptr->ordinal!=tmp_ptr2->ordinal) {
                if(strcmp(tmp_ptr->alias,tmp_ptr2->alias)==0){
                    err_msg=malloc(1024);
                    sprintf(err_msg,"Ambiguous column in select expression: %s at ordinal %d\n",tmp_ptr->alias,tmp_ptr2->ordinal);
                    set_error(cursor,ERR_AMBIGUOUS_COLUMN_IN_SELECT_LIST,err_msg);
                    return 0;
                }
            }
            tmp_ptr2=tmp_ptr2->next;
        }
        tmp_ptr=tmp_ptr->next;
    }

    // fixup join/from qualifier
    // if the database isn't set.. use the active database as the qualifier 
    if(select->from) {
        if(select->from->qualifier==0) select->from->qualifier=get_current_database(cursor);
        join_t *join_ptr=0;
        for(int i=0;i<select->join_length;i++) {
            join_ptr=&select->join[i];
            if(join_ptr->identifier->qualifier==0) join_ptr->identifier->qualifier=get_current_database(cursor);
        }
    }

    // fixup join/from alias 
    if(select->from) {
        if(select->alias==0) select->alias=string_duplicate((char *)select->from->source);
        join_t *join_ptr=0;
        for(int i=0;i<select->join_length;i++) {
            join_ptr=&select->join[i];
            if(join_ptr->alias==0) join_ptr->alias=string_duplicate((char *)join_ptr->identifier->source);
        }
    }
    
    // validate join/from ambiguity
    if(select->from) {
        join_t *join_ptr=0;
        join_t *join_ptr2=0;
        for(int i=0;i<select->join_length;i++) {
            join_ptr=&select->join[i];
            
            // join and from ambiguity validation
            if(strcmp(join_ptr->alias,select->alias)==0) {
                err_msg=malloc(1024);
                sprintf(err_msg,"ambiguous join: %s",join_ptr->alias);
                set_error(cursor,ERR_AMBIGUOUS_JOIN,err_msg);
                return 0;
            }

            for(int j=0;j<select->join_length;j++) {
                // skip self match
                if(j==i) continue;
                join_ptr2=&select->join[j];
                // unique match
                if(strcmp(join_ptr->alias,join_ptr2->alias)==0) {
                    err_msg=malloc(1024);
                    sprintf(err_msg,"ambiguous join: %s",join_ptr->alias);
                    set_error(cursor,ERR_AMBIGUOUS_JOIN,err_msg);
                    return 0;
                }
            
            }
            
        }
    }

    // validate from and join sources exist
    if(select->from) {
        table_def_t *table_ptr=0;
        table_ptr=get_table_by_identifier(cursor,select->from);
        if(table_ptr==0) {
            err_msg=malloc(1024);
            sprintf(err_msg,"invalid FROM table: %s.%s",select->from->qualifier,select->from->source);
            set_error(cursor,ERR_INVALID_FROM_TABLE,err_msg);
            return 0;
        }
        join_t *join_ptr=0;
        for(int i=0;i<select->join_length;i++) {
            join_ptr=&select->join[i];
            table_ptr=get_table_by_identifier(cursor,join_ptr->identifier);
            if(table_ptr==0) {
                err_msg=malloc(1024);
                sprintf(err_msg,"invalid JOIN table: %s.%s",join_ptr->identifier->qualifier,join_ptr->identifier->source);
                set_error(cursor,ERR_INVALID_JOIN_TABLE,err_msg);
                return 0;
            }
        }
    }

    // at this point. 
    // all FROM/JOIN sources exist and are unique
    // all select columns are UNIQUE
    // now we validate that all SELECT identifiers exist in the FROM/JOIN
    if(select->from) {
        tmp_ptr=select->columns;
        int found=0;
        while(tmp_ptr){
            // we only care about data sourced from tables
            if (tmp_ptr->type==TOKEN_IDENTIFIER) {
                identifier_t *temp_ident=(identifier_t*)tmp_ptr->object;
                //printf("LOOKING FOR\n");
                //debug_identifier(temp_ident);
                // ok we know exactly where we are getting this data from... validate column.
                if(temp_ident->qualifier) {
                    //printf("QUALIFIER \n");
                    // is it in the from?
                    table_def_t *temp_table=0;
                    
                    if(strcmp(temp_ident->qualifier,select->alias)==0) {
                        //printf("CHECK FROM---QUALIFIER %s : FROM %s\n",temp_ident->qualifier,select->alias);
                        temp_table=get_table_by_identifier(cursor,select->from);
                    } else {
                        join_t *tmp_join=select->join;
                        int len=select->join_length;
                        for(int i=0;i<len;i++){
                            if(strcmp(tmp_join[i].alias,temp_ident->qualifier)==0){
                                //printf("CHECK JOIN---QUALIFIER %s : JOIN %s\n",temp_ident->qualifier,tmp_join[i].alias);
                                temp_table=get_table_by_identifier(cursor,tmp_join[i].identifier);
                                break;
                            }
                        }// end for
                    }// end else
                    
                    // we didnt find the referenced qualifier as a source 
                    if (temp_table==0) {
                        err_msg=malloc(1024);
                        sprintf(err_msg,"invalid qualifier in SELECT: %s",temp_ident->qualifier);
                        set_error(cursor,ERR_INVALID_QUALIFIER,err_msg);
                        return 0;
                    }


                    data_column_t* tmp_ptr2=temp_table->columns;
                    found=table_has_column(temp_table,temp_ident->source);
                    // loop from the found table columns

                    if(found==0) {
                        err_msg=malloc(1024);
                        sprintf(err_msg,"invalid column `%s` in table: `%s`.`%s`",temp_ident->source,temp_table->identifier->qualifier,temp_table->identifier->source);
                        set_error(cursor,ERR_COLUMN_NOT_FOUND,err_msg);
                        return 0;
                    }
                   
                } else {
                // lets search all the sources for this column... and make sure its unique
                    //printf("NO QUALIFIER \n");
                    table_def_t *temp_table=get_table_by_identifier(cursor,select->from);
                    found=0;
                    found+=table_has_column(temp_table,temp_ident->source);
                    char *qualifier=select->alias;

                    join_t *tmp_join=select->join;
                    int len=select->join_length;
                    for(int i=0;i<len;i++){
                        temp_table=get_table_by_identifier(cursor,tmp_join[i].identifier);
                        found+=table_has_column(temp_table,temp_ident->source);
                        if(found==1) qualifier=tmp_join[i].alias;
                    }
                    //printf("%d\n",found);
                    if(found==0) {
                        err_msg=malloc(1024);
                        sprintf(err_msg,"invalid column `%s` in select",temp_ident->source);
                        set_error(cursor,ERR_COLUMN_NOT_FOUND,err_msg);
                        return 0;
                    }
                    if(found>1) {
                        err_msg=malloc(1024);
                        sprintf(err_msg,"ambiguious column `%s` in select",temp_ident->source);
                        set_error(cursor,ERR_AMBIGUOUS_COLUMN_IN_SELECT_LIST,err_msg);
                        return 0;
                    }
                    if(found==1) {
                        *temp_ident->qualifier=strdup(qualifier);
                    }
                }
            }
            tmp_ptr=tmp_ptr->next;
        }
    }

    // At this point the select list, from and join sources have 
    // been validated to be legal, and non ambiguious.

    

    // where check
    // identifier must be in from/target
    // expression must validate

    // group check
    // columns must exist in select
    // columns must be unique

    // order check
    // columns must exist in select
    // columns must be unique
   /*
    expression_t *tmp_expr=select->group;
    while(tmp_expr){
        data_column_t *temp_dc=select->columns;
        while(temp_dc){
            // if there is a qualifier.. its the local identifier

            if(tmp_expr->identifier->qualifier) {

            } else {
            // else its just a colu
            }
            
        }

        tmp_expr=tmp_expr->expression;
    }
*/
    return 0;
}

