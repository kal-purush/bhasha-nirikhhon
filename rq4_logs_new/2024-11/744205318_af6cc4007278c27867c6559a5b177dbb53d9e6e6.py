from app.db.session import connect_to_db
from neo4j import Driver


class Author:
    @connect_to_db
    def get(self, id, db, type=None):
        """

        Notes
        -----
        MATCH (a:Author)
        RETURN a.first_name as first_name, a.last_name as last_name,
               p.name as affiliation;
        MATCH (a:Author)-[r:author_of]->(p:Article)
        OPTIONAL MATCH (a:Author)-[:member_of]->(p:Partner)
        WHERE a.uuid = $uuid
        RETURN *;

        """
        author_query = """MATCH (a:Author) WHERE a.uuid = $uuid
                          OPTIONAL MATCH (a)-[:member_of]->(p:Partner)
                          OPTIONAL MATCH (a)-[:member_of]->(u:Workstream)
                          RETURN a.uuid as uuid, a.orcid as orcid,
                          a.first_name as first_name, a.last_name as last_name,
                          collect(p.id, p.name) as affiliations,
                          collect(u.id, u.name) as workstreams;
                          """
        author, summary, keys = db.execute_query(author_query, uuid=id)
        results = author[0].data()

        collab_query = """MATCH (a:Author)-[r:author_of]->(p:Output)<-[s:author_of]-(b:Author)
                          WHERE a.uuid = $uuid AND b.uuid <> $uuid
                          RETURN DISTINCT b.uuid as uuid, b.first_name as first_name, b.last_name as last_name, b.orcid as orcid
                          LIMIT 5"""
        colabs, summary, keys = db.execute_query(collab_query, uuid=id)

        results["collaborators"] = colabs

        if type and type in ["publication", "dataset", "software", "other"]:
            publications_query = """MATCH (a:Author)-[:author_of]->(p:Output)
                                    WHERE (a.uuid) = $uuid AND (p.result_type = $type)
                                    CALL {
                                        WITH p
                                        MATCH (b:Author)-[r:author_of]->(p)
                                        RETURN b
                                        ORDER BY r.rank
                                    }
                                    OPTIONAL MATCH (p)-[:REFERS_TO]->(c:Country)
                                    RETURN p as outputs,
                                           collect(DISTINCT c) as countries,
                                           collect(DISTINCT b) as authors
                                    ORDER BY outputs.publication_year DESCENDING;"""
        else:
            publications_query = """MATCH (a:Author)-[:author_of]->(p:Output)
                                    WHERE a.uuid = $uuid
                                    CALL {
                                        WITH p
                                        MATCH (b:Author)-[r:author_of]->(p)
                                        RETURN b
                                        ORDER BY r.rank
                                    }
                                    OPTIONAL MATCH (p)-[:REFERS_TO]->(c:Country)
                                    RETURN p as outputs,
                                        collect(DISTINCT c) as countries,
                                        collect(DISTINCT b) as authors
                                    ORDER BY outputs.publication_year DESCENDING;"""
        result, summary, keys = db.execute_query(publications_query, uuid=id, type=type)
        results["outputs"] = [x.data() for x in result]

        return results

    @connect_to_db
    def count(self, id: str, db: Driver):
        """Returns counts of the result types

        Arguments
        ---------
        db : Driver
        """
        query = """
                MATCH (a:Author)-[b:author_of]->(o:Article)
                WHERE (a.uuid) = $uuid
                RETURN o.result_type as result_type, count(o) as count
                """
        records, summary, keys = db.execute_query(query, uuid=id)
        return {x.data()["result_type"]: x.data()["count"] for x in records}


class AuthorList:
    @connect_to_db
    def get(self, db):
        """

        Notes
        -----

        """
        query = """MATCH (a:Author)
                   OPTIONAL MATCH (a)-[:member_of]->(p:Partner)
                   OPTIONAL MATCH (a)-[:member_of]->(u:Workstream)
                   RETURN a.first_name as first_name, a.last_name as last_name, a.uuid as uuid, a.orcid as orcid, collect(p.id, p.name) as affiliation, collect(u.id, u.name) as workstreams
                   ORDER BY last_name;
                   """
        records, summary, keys = db.execute_query(query)

        return records