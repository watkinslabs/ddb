

long mycmp(const unsigned char *cmp1, const unsigned char *cmp2, unsigned long length) {
    if(length >= 4) {
        long difference = *(unsigned long *)cmp1 - *(unsigned long *)cmp2;
        if(difference)
            return difference;
    }
    
    return memcmp(cmp1,cmp2,length);
}

int compare_identifiers(identifier_t *source,identifier_t *dest){
    if(source==0) return 0;
    if(dest  ==0) return 0;
    if(source->qualifier==0) return 0;
    if(dest  ->qualifier==0) return 0;
    if(source->source   ==0) return 0;
    if(dest  ->source   ==0) return 0;
    //printf("\n - COMPARE IDENTIFIER \n");
    //debug_identifier(source);
    //debug_identifier(dest);
    int len_src= strlen(source->source);
    int len_qual=strlen(source->qualifier);
    if(len_src!=strlen(dest->source)) return 0;
    if(len_qual!=strlen(dest->qualifier)) return 0;

    if (mycmp(source->qualifier,dest->qualifier,len_qual)==0 && 
        mycmp(source->source,dest->source,len_src)==0) return 1;
    return 0;
}

int compare_literals(token_t *source,token_t *dest){
    if (strcmp(source->value,dest->value)==0) return 1;
    return 0;
}
