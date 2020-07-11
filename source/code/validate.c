#include "../include/validate.h"
#include "../include/errors.h"
#include "../include/debug.h"
#include "../include/queries.h"
#include "../include/free.h"
#include "../include/core.h"
#include <string.h>
#include <stdio.h>
//#include <io.h>


int is_identifier_valid(cursor_t * cursor,select_t *select,identifier_t *ident,char *section){
    
     char *err_msg=0;
    // at this point. 
    // all FROM/JOIN sources exist and are unique
    // all select columns are UNIQUE
    // now we can validate that all identifiers exist in the FROM/JOIN
    
    // skip null idents
    if(ident==0)  return 0;
    
    //printf(" %s",section);
    //debug_identifier(ident);
    if(select->from) {
        int found=0;
        // we only care about data sourced from tables
        if(ident->qualifier) {
            // is it in the from?
            table_def_t *temp_table=0;
            
            if(strcmp(ident->qualifier,select->alias)==0) {
                temp_table=get_table_by_identifier(cursor,select->from);
            } else {
                join_t *tmp_join=select->join;
                int len=select->join_length;
                for(int i=0;i<len;i++){
                    if(strcmp(tmp_join[i].alias,ident->qualifier)==0){
                        temp_table=get_table_by_identifier(cursor,tmp_join[i].identifier);
                        break;
                    }
                }// end for
            }// end else
            
            // we didnt find the referenced qualifier as a source 
            if (temp_table==0) {
                err_msg=malloc(1024);
                SPRINTF(err_msg,"%s: invalid qualifier: %s",section,ident->qualifier);
                set_error(cursor,ERR_INVALID_QUALIFIER,err_msg);
                return 0;
            }


            found=table_has_column(temp_table,ident->source);
            // loop from the found table columns

            if(found==0) {
                err_msg=malloc(1024);
                SPRINTF(err_msg,"%s: invalid column `%s`.`%s` in table: `%s`.`%s`",section,ident->qualifier,ident->source,temp_table->identifier->qualifier,temp_table->identifier->source);
                set_error(cursor,ERR_COLUMN_NOT_FOUND,err_msg);
                return 0;
            } 
            if(found==1) {
                return 1;
            }
        } else {
        // lets search all the sources for this column... and make sure its unique
            //printf("NO QUALIFIER \n");
            //debug_identifier(ident);
            table_def_t *temp_table=get_table_by_identifier(cursor,select->from);
            found=table_has_column(temp_table,ident->source);
            char *qualifier=select->alias;

            join_t *tmp_join=select->join;
            int len=select->join_length;
            for(int i=0;i<len;i++){
                temp_table=get_table_by_identifier(cursor,tmp_join[i].identifier);
                int res=table_has_column(temp_table,ident->source);
                found+=res;
                if(res==1) qualifier=tmp_join[i].alias;
            }
            //printf("%d\n",found);
            if(found==0) {
                err_msg=malloc(1024);
                SPRINTF(err_msg,"%s: invalid column `%s`",section,ident->source);
                set_error(cursor,ERR_COLUMN_NOT_FOUND,err_msg);
                return 0;
            }
            if(found>1) {
                err_msg=malloc(1024);
                SPRINTF(err_msg,"%s: ambiguious column `%s`, %d found",section,ident->source,found);
                set_error(cursor,ERR_AMBIGUOUS_COLUMN_NAME,err_msg);
                return 0;
            }
            if(found==1) {
                ident->qualifier=STRDUP(qualifier);
                return 1;
            }
        }// end else

    }// end if for
    printf("Err\n");
    return 0;
}// end func

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

int column_index_in_table(table_def_t *table,char *column){
    data_column_t *tmp_ptr=table->columns;
    long column_index=0;
    while(tmp_ptr){                            
        if(strcmp(tmp_ptr->object,column)==0){
            return column_index;
        }
        ++column_index;
        tmp_ptr=tmp_ptr->next;
    }
    return -1;
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
                SPRINTF(msg,"Table already exists %s.%s",table->identifier->qualifier,table->identifier->source);
                set_error(cursor,ERR_TABLE_ALREADY_EXISTS,msg);
                return 0;
            }
        }
        next=next->next;
    }
    // check to see if file is accessable
    if( ACCESS( table->file, F_OK) != -1 ) {
        if( ACCESS( table->file, R_OK) != -1 ) {
            if(ACCESS( table->file, W_OK) != -1 ) {
            } else {
                msg=safe_malloc(1000,1);
                SPRINTF(msg,"Cant write to file %s",table->file);
                set_error(cursor,ERR_FILE_WRITE_PERMISSION,msg);
                return 0;
            }
        } else {
            msg=safe_malloc(1000,1);
            SPRINTF(msg,"Cant read from file %s",table->file);
            set_error(cursor,ERR_FILE_READ_PERMISSION,msg);
            return 0;
        }
    } else {
        msg=safe_malloc(1000,1);      
        SPRINTF(msg,"Cant find file %s",table->file);
        set_error(cursor,ERR_FILE_NOT_FOUND,msg);
        return 0;
    }
    //check to see if table has columns and they are uniquely named

    if(table->columns==0) {
        msg=safe_malloc(1000,1);      
        SPRINTF(msg,"no columns in %s.%s",table->identifier->qualifier,table->identifier->source);
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
                        SPRINTF(msg,"Column must be a unique literal %s",(char*)inner_tmp->object);
                        set_error(cursor,ERR_AMBIGUOUS_COLUMN_NAME,msg);
                        return 0;
                    }
                }
                ++inner_index;
                inner_tmp=inner_tmp->next;
            }

        } else {
            msg=safe_malloc(1000,1);      
            SPRINTF(msg,"Column must be a unique literal");
            set_error(cursor,ERR_INVALID_COLUMN_NAME,msg);
            return 0;
        }
        outer_tmp=outer_tmp->next;
        ++outer_index;
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
            SPRINTF(err_msg,"database not specified");
            set_error(cursor,ERR_INVALID_DATABASE,err_msg);
            return 0;
        }

        // check to make sure the table exists.. a table must contain that qualifier/db name
        table_def_t *temp_table=cursor->tables;
        while(temp_table){
            if(strcmp(temp_table->identifier->qualifier,use->database)==0) {
                return 1;
            }
            temp_table=temp_table->next;
        }
        char *err_msg=malloc(1024);
        SPRINTF(err_msg,"database not found `%s`",use->database);
        set_error(cursor,ERR_INVALID_DATABASE,err_msg);
        return 0;
    }
    return 0;
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
    // TODO add ERROR OUTPUT.... TO CURSOR....
   // count columns and create an array
    data_column_t *tmp_ptr=select->columns;
    int column_length=0;
    while(tmp_ptr){
        ++column_length;
        tmp_ptr=tmp_ptr->next;
    }
    select->column_length=column_length;

    if(column_length==0) {
        err_msg=malloc(1024);
        SPRINTF(err_msg,"Select missing select list/columns");
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
                            SPRINTF(err_msg,"Unknown Select token in select list: %s",token_type(tmp_ptr->type));
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
                    SPRINTF(err_msg,"Ambiguous column in select expression: %s at ordinal %d\n",tmp_ptr->alias,tmp_ptr2->ordinal);
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
                SPRINTF(err_msg,"ambiguous join: %s",join_ptr->alias);
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
                    SPRINTF(err_msg,"ambiguous join: %s",join_ptr->alias);
                    set_error(cursor,ERR_AMBIGUOUS_JOIN,err_msg);
                    return 0;
                }
            
            }
            
        }
    }

    //temp code just show identifiers in select
    /*tmp_ptr=select->columns;
    while(tmp_ptr){
        // we only care about data sourced from tables
        if (tmp_ptr->type==TOKEN_IDENTIFIER) {
            identifier_t *sel_ident=(identifier_t*)tmp_ptr->object;
            printf("HI\n");
            debug_identifier(sel_ident);
        }
        tmp_ptr=tmp_ptr->next;
    }*/

    if(select->from) {
        tmp_ptr=select->columns;
        int found=0;
        while(tmp_ptr){
            // we only care about data sourced from tables
            if (tmp_ptr->type==TOKEN_IDENTIFIER) {
                //debug_identifier((identifier_t*)tmp_ptr->object);
                int res=is_identifier_valid(cursor,select,(identifier_t*)tmp_ptr->object,"select list");

                if(res==0) {
                    return 0;
                }
            }
            tmp_ptr=tmp_ptr->next;
        }
    }

// validate from and join sources exist
    if(select->from) {
        table_def_t *table_ptr=0;
        table_ptr=get_table_by_identifier(cursor,select->from);
        if(table_ptr==0) {
            err_msg=malloc(1024);
            SPRINTF(err_msg,"invalid FROM table: %s.%s",select->from->qualifier,select->from->source);
            set_error(cursor,ERR_INVALID_FROM_TABLE,err_msg);
            return 0;
        }
        join_t *join_ptr=0;
        for(int i=0;i<select->join_length;i++) {
            join_ptr=&select->join[i];
            table_ptr=get_table_by_identifier(cursor,join_ptr->identifier);
            if(table_ptr==0) {
                err_msg=malloc(1024);
                SPRINTF(err_msg,"invalid JOIN table: %s.%s",join_ptr->identifier->qualifier,join_ptr->identifier->source);
                set_error(cursor,ERR_INVALID_JOIN_TABLE,err_msg);
                return 0;
            }
        }
    }

    // at this point. 
    // all FROM/JOIN sources exist and are unique
    // all select columns are UNIQUE
    // now we validate that all SELECT identifiers exist in the FROM/JOIN


    // populate aliases of sources 
    if(select->from) {
        //init array block
        cursor->source_alias=(char **)safe_malloc(sizeof(char*),select->join_length+1);
        table_def_t *table_ptr=0;
        table_ptr=get_table_by_identifier(cursor,select->from);
        if(table_ptr==0) {
            err_msg=malloc(1024);
            SPRINTF(err_msg,"invalid FROM table: %s.%s",select->from->qualifier,select->from->source);
            set_error(cursor,ERR_INVALID_FROM_TABLE,err_msg);
            return 0;
        }
        cursor->source_alias[0]=STRDUP(select->alias);
        join_t *join_ptr=0;
        for(int i=0;i<select->join_length;i++) {
            join_ptr=&select->join[i];
            table_ptr=get_table_by_identifier(cursor,join_ptr->identifier);
            if(table_ptr==0) {
                err_msg=malloc(1024);
                SPRINTF(err_msg,"invalid JOIN table: %s.%s",join_ptr->identifier->qualifier,join_ptr->identifier->source);
                set_error(cursor,ERR_INVALID_JOIN_TABLE,err_msg);
                return 0;
            }
            cursor->source_alias[i+1]=STRDUP(select->join[i].alias);
        }
    }


    //create lookup for identifiers.. speed things up
    /*
    if(select->from) {
        cursor->identifier_count=select->column_length;
        cursor->identifier_lookup=safe_malloc(sizeof(identifier_lookup_t),select->column_length);
        tmp_ptr=select->columns;
        int found=0;
        int index=0;
        while(tmp_ptr){
            // we only care about data sourced from tables
            
            if (tmp_ptr->type==TOKEN_IDENTIFIER) {
                for(int i=0;i<select->join_length+1;i++) {
                    identifier_t *sel_ident=(identifier_t*)tmp_ptr->object;

                    if(strcmp(sel_ident->qualifier,cursor->source_alias[i])==0) {
                        cursor->identifier_lookup[index].active=1;
                        cursor->identifier_lookup[index].source=i;
                        table_def_t *table_ptr;
                        identifier_t *src_ident=0;
                        
                        if(i==0) {
                            src_ident=select->from;
                        } else {
                            src_ident=select->join[i-1].identifier;
                        }
                        table_ptr=get_table_by_identifier(cursor,src_ident);

                        if(table_ptr==0) {
                            debug_identifier(src_ident);
                            err_msg=malloc(1024);
                            SPRINTF(err_msg,"invalid table");
                            set_error(cursor,ERR_INVALID_FROM_TABLE,err_msg);
                            return 0;
                        }
                        
                        cursor->identifier_lookup[index].select_column=index;
                        cursor->identifier_lookup[index].identifier=duplicate_identifier((identifier_t*)tmp_ptr->object);
                        cursor->identifier_lookup[index].source_column=column_index_in_table(table_ptr,sel_ident->source);
                        break;
                    }
                }
            }
            ++index;
            tmp_ptr=tmp_ptr->next;
        }
    }*/

    // At this point the select list, from and join sources have 
    // been validated to be legal, and non ambiguious.

    // where identifier check
    // identifier must be in from/target
    if(select->where){
        expression_t *temp_expr=select->where;

        while(temp_expr){
            if(temp_expr->mode==1) {
                int res=is_identifier_valid(cursor,select,temp_expr->identifier,"where");
                if(res==0) {
                    return 0;                    
                }
            }
            temp_expr=temp_expr->expression;
        }
    }

    cursor->identifier_count=100;
    cursor->identifier_lookup=safe_malloc(sizeof(identifier_lookup_t),cursor->identifier_count);
    cursor->identifier_count=0;
    
    if(select->from) {
        tmp_ptr=select->columns;
        while(tmp_ptr){
            if (tmp_ptr->type==TOKEN_IDENTIFIER) {
                add_identifier_to_cursor_lookup(cursor,select,(identifier_t *)tmp_ptr->object);
            }
            tmp_ptr=tmp_ptr->next;
        }
    }
    if(select->join) {
        for(int i=0;i<select->join_length;i++) {
            expression_t *temp_expr=select->join[i].expression;
            while(temp_expr){
                if (temp_expr->mode==1) {
                    add_identifier_to_cursor_lookup(cursor,select,temp_expr->identifier);
                }
                temp_expr=temp_expr->expression;
            }
        }
    }
    if(select->where) {
        expression_t *temp_expr=select->where;
        while(temp_expr){
            
            if (temp_expr->mode==1) {
                //debug_identifier(temp_expr->identifier);

                add_identifier_to_cursor_lookup(cursor,select,temp_expr->identifier);
            }
            temp_expr=temp_expr->expression;
        }
    }


    //join identifier check
    // identifier must be in from/target
    if(select->join) {
         for(int i=0;i<select->join_length;i++) {
            join_t *join_ptr=&select->join[i];
            expression_t *temp_expr=join_ptr->expression;

            while(temp_expr){
                if(temp_expr->mode==1) {
                    int res=is_identifier_valid(cursor,select,temp_expr->identifier,"join");
                    if(res==0) {
                        return 0;                    
                    }
                }
                temp_expr=temp_expr->expression;
            }
         }
    }
    // expression must validate


    // group check
    // columns must exist in from/join
    if(select->group){
        expression_t *tmp_expr=select->group;
        while(tmp_expr){
            int res=is_identifier_valid(cursor,select,tmp_expr->identifier,"group by");
            if(res==0) return 0;
            tmp_expr=tmp_expr->expression;
        }
        
        // columns must be unique
        tmp_expr=select->group;
        int index1=0;
        while(tmp_expr){
            expression_t *tmp_expr2=select->group;
            int index2=0;
            while(tmp_expr2){
                if(index1!=index2) {
                    if(compare_identifiers(tmp_expr->identifier,tmp_expr2->identifier)) {
                        err_msg=malloc(1024);
                        SPRINTF(err_msg,"duplicate group by column: %s.%s",tmp_expr2->identifier->qualifier,tmp_expr2->identifier->source);
                        set_error(cursor,ERR_DUPLICATE_GROUP_BY_COLUMN,err_msg);
                        return 0;
                    }
                }
                tmp_expr2=tmp_expr2->expression;
                ++index2;
            }
            tmp_expr=tmp_expr->expression;
            ++index1;
        }
    }

    if(select->order) {
        expression_t *tmp_expr=select->order;
        while(tmp_expr){
            int res=is_identifier_valid(cursor,select,tmp_expr->identifier,"order by");
            if(res==0) return 0;
            tmp_expr=tmp_expr->expression;
        }
        
        // columns must be unique
        tmp_expr=select->order;
        int index1=0;
        while(tmp_expr){
            expression_t *tmp_expr2=select->order;
            int index2=0;
            while(tmp_expr2){
                if(index1!=index2) {
                    if(compare_identifiers(tmp_expr->identifier,tmp_expr2->identifier)) {
                        err_msg=malloc(1024);
                        SPRINTF(err_msg,"duplicate order by column: %s.%s",tmp_expr2->identifier->qualifier,tmp_expr2->identifier->source);
                        set_error(cursor,ERR_DUPLICATE_GROUP_BY_COLUMN,err_msg);
                        return 0;
                    }
                }
                tmp_expr2=tmp_expr2->expression;
                ++index2;
            }
            tmp_expr=tmp_expr->expression;
            ++index1;
        }
    }

    if(select->limit_start) {
        if(select->has_limit_start && select->limit_start<0) {
            err_msg=malloc(1024);
            SPRINTF(err_msg,"limit: start index cannot be negative: %d",select->limit_start);
            set_error(cursor,ERR_LIMIT_START_NEGATIVE,err_msg);
            return 0;
        }
        if(select->has_limit_length && select->limit_length<0) {
            err_msg=malloc(1024);
            SPRINTF(err_msg,"limit: length index cannot be negative: %d",select->limit_length);
            set_error(cursor,ERR_LIMIT_LENGTH_NEGATIVE,err_msg);
            return 0;
        }
    }
    return 1;
}

int add_identifier_to_cursor_lookup(cursor_t *cursor,select_t *select,identifier_t *ident){
    int index=cursor->identifier_count;
    ++cursor->identifier_count;
    //debug_identifier(ident);
    for(int i=0;i<select->join_length+1;i++) {
        if(strcmp(ident->qualifier,cursor->source_alias[i])==0) {
            cursor->identifier_lookup[index].active=1;
            cursor->identifier_lookup[index].source=i;
            table_def_t *table_ptr;
            identifier_t *src_ident=0;
            
            if(i==0) {
                src_ident=select->from;
            } else {
                src_ident=select->join[i-1].identifier;
            }
            table_ptr=get_table_by_identifier(cursor,src_ident);

            if(table_ptr==0) {
                debug_identifier(src_ident);
                char *err_msg=malloc(1024);
                SPRINTF(err_msg,"invalid table");
                set_error(cursor,ERR_INVALID_FROM_TABLE,err_msg);
                return 0;
            }
            
            cursor->identifier_lookup[index].select_column=index;
            cursor->identifier_lookup[index].identifier=duplicate_identifier(ident);
            cursor->identifier_lookup[index].source_column=column_index_in_table(table_ptr,ident->source);
        }
    }
    return 1;
}