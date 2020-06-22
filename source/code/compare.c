int compare_identifiers(identifier_t *source,identifier_t *dest){
    if(source==0) return 0;
    if(dest  ==0) return 0;
    if(source->qualifier==0) return 0;
    if(dest  ->qualifier==0) return 0;
    if(source->source   ==0) return 0;
    if(dest  ->source   ==0) return 0;
    //printf("\n - COMPARE IDENTIFIER \n");
    debug_identifier(source);
    debug_identifier(dest);

    if (strcmp(source->qualifier,dest->qualifier)==0 && 
        strcmp(source->source,dest->source)==0) return 1;
    return 0;
}

int compare_literals(token_t *source,token_t *dest){
    if (strcmp(source->value,dest->value)==0) return 1;
    return 0;
}
