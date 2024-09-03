//PREMIUM FEATURE:
//build your own tree (free tier, build and get it in a day or so)

//NEEDED FEATURE:
//add paper as breakdown level
//add author as breakdown level
//add journal field as root
//one new kind of viz - (map? pca?)

//CODE:
//rules!
//different entities -> needed as root, can go to static, needs to go with tree, etc.
//tree att only built with fix on top? - not really beacuse q - source not fix
//  but needs to be fix on paper level
//fix atts generic fix
//  why tho?

//CONFIG:
//tree-like jsony thing where Child =  {id: string, children: Child[], isNextLevel:bool, buildsOn: string?} or something like that

//write triples for a BBN (Breakdown Basis Network)
//  -> render (compile) BBNs
//write aggregations + OAPs (Ordered Aggregation Points)
//+partition bases (like year)  - in this case paritions get "folded", not that somehow too
//++++ note some sort of filter/modification of edges, like in the case of coauthors/collaborations
// number of authors / institutions +
// more sophisticated agg, like unique authors with at least 10 papers

fn get_author_qc_spec_bases() {
    ReferencedAuthor;
    (RefrencedSubField, CitingSubfield, CitingTopic);
    (CitingSubfield, CitingTopic, CitingSource);
    (ReferencedQs, CitingSubfield, CitingCountry);
    (
        CitingCountry,
        CitingInstitution,
        CitingSubfield,
        CitingTopic,
    );
    (CitingSource, CitingCountry, CitingInstitution);
}

fn get_inst_qc_spec_bases() {
    ReferencedInstitution;
    (RefrencedSubField, CitingSubfield, CitingTopic);
    (
        ReferencedQs,
        ReferencedSource,
        CitingSubfield,
        CitingCountry,
    );
    (
        CitingCountry,
        CitingInstitution,
        CitingSubfield,
        CitingTopic,
    );
    (
        ReferencedSubfield,
        CitingCountry,
        CitingInstitution,
        CitingSubfield,
    );
    (CitingSubfield, CitingTopic, CitingSource);
    (CitingSource, CitingCountry, CitingSubfield);
    (
        ReferencedNonAffiliatedCountry,
        ReferencedSubfield,
        ReferencedNonAffiliatedInstitution,
        ReferencedTopic,
    );
    (ReferencedAffiliatedAuthor, CitingCountry, CitingInstitution);
    (ReferencedTopic, ReferencedQs, ReferencedSource);
}

// countries all of the above

fn get_areafield_qc_spec_bases() {
    ReferencedAreaField;
    (RefrencedAreaCategorizedSource, CitingSubfield, CitingTopic);
    (
        ReferencedCountry,
        ReferencedAreaCategorizedSource,
        ReferencedTopic,
    );
}
