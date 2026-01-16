package com.nickrobison.trestle.reasoner.engines.relations;

import com.nickrobison.trestle.common.TrestlePair;
import com.nickrobison.trestle.ontology.ITrestleOntology;
import com.nickrobison.trestle.reasoner.engines.object.ITrestleObjectReader;
import com.nickrobison.trestle.reasoner.engines.object.ITrestleObjectWriter;
import com.nickrobison.trestle.reasoner.engines.spatial.SpatialComparisonReport;
import com.nickrobison.trestle.reasoner.engines.spatial.SpatialEngine;
import com.nickrobison.trestle.reasoner.engines.temporal.TemporalComparisonReport;
import com.nickrobison.trestle.reasoner.engines.temporal.TemporalEngine;
import com.nickrobison.trestle.reasoner.parser.TrestleParser;
import com.nickrobison.trestle.transactions.TrestleTransaction;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.semanticweb.owlapi.model.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.Temporal;
import java.util.Objects;
import java.util.Optional;

import static com.nickrobison.trestle.common.IRIUtils.parseStringToIRI;

/**
 * Created by nickrobison on 2/12/21
 */
public class RelationCalculator<T> {

    private static final Logger logger = LoggerFactory.getLogger(RelationCalculator.class);

    private final ITrestleOntology ontology;
    private final String reasonerPrefix;
    private final ITrestleObjectReader objectReader;
    private final ITrestleObjectWriter objectWriter;
    private final SpatialEngine spatialEngine;
    private final RelationTracker relationTracker;
    private final TrestleParser trestleParser;
    private final TemporalEngine temporalEngine;
    private final Class<T> clazz;
    private final String individual;
    private final @Nullable Temporal validAt;

    public RelationCalculator(ITrestleOntology ontology, String reasonerPrefix, ITrestleObjectReader objectReader, ITrestleObjectWriter objectWriter, SpatialEngine spatialEngine, RelationTracker tracker, TemporalEngine temporalEngine, TrestleParser parser, Class<T> clazz, String individual, @Nullable Temporal validAt) {
        this.ontology = ontology;
        this.reasonerPrefix = reasonerPrefix;
        this.objectReader = objectReader;
        this.objectWriter = objectWriter;
        this.spatialEngine = spatialEngine;
        this.relationTracker = tracker;
        this.trestleParser = parser;
        this.temporalEngine = temporalEngine;
        this.clazz = clazz;
        this.individual = individual;
        this.validAt = validAt;
    }

    public Completable calculate() {
        final IRI individualIRI = parseStringToIRI(this.reasonerPrefix, individual);
//        Read the object first
        final TrestleTransaction trestleTransaction = this.ontology.createandOpenNewTransaction(true);
        return this.objectReader.readTrestleObject(clazz, individualIRI, false, validAt, null, trestleTransaction)
                .flatMapCompletable(trestleObject -> intersectObjects(individualIRI, trestleObject, trestleTransaction))
                .doOnComplete(() -> this.ontology.returnAndCommitTransaction(trestleTransaction))
                .doOnError(error -> this.ontology.returnAndAbortTransaction(trestleTransaction));
    }

    private Completable intersectObjects(IRI individualIRI, T trestleObject, TrestleTransaction trestleTransaction) {
        return this.spatialEngine.spatialIntersectObject(trestleObject, 1, validAt, null, trestleTransaction)
                .filter(object -> shouldCompute(individualIRI, object))
                .map(intersectedObject -> buildTrestlePair(individualIRI, trestleObject, intersectedObject))
                .filter(Objects::nonNull)
                .filter(pair -> !pair.getRight().getLeft().getRelations().isEmpty())
                .flatMapCompletable(cPair -> writeRelationships(individualIRI, trestleObject, cPair, trestleTransaction));
    }

    private Completable writeRelationships(IRI individualIRI, T trestleObject, TrestlePair<T, TrestlePair<SpatialComparisonReport, TemporalComparisonReport>> cPair, TrestleTransaction trestleTransaction) {
        final T intersectedObject = cPair.getLeft();
        final SpatialComparisonReport spatialComparisonReport = cPair.getRight().getLeft();
        final TemporalComparisonReport temporalComparisonReport = cPair.getRight().getRight();
        final IRI intersectedObjectID = parseStringToIRI(this.reasonerPrefix, spatialComparisonReport.getObjectBID());

        //                Write the relationships
        final Completable relationsCompletable = Observable.fromIterable(spatialComparisonReport.getRelations())
                .flatMapCompletable(relation -> {
                    logger.trace("Writing spatial relationship {}", relation);
                    final TrestleTransaction tt = this.ontology.createandOpenNewTransaction(trestleTransaction);
                    return this.objectWriter.writeObjectRelationship(trestleObject, intersectedObject, relation, tt)
                            .doOnComplete(() -> this.ontology.returnAndCommitTransaction(tt))
                            .doOnError(error -> this.ontology.returnAndAbortTransaction(tt));
                });

        final Completable spatialComparisonCompletable = writeSpatialComparison(trestleObject, intersectedObject, spatialComparisonReport, trestleTransaction);


//                Write overlaps
        spatialComparisonReport.getSpatialOverlap().ifPresent(overlap -> {
            logger.debug("Writing spatial overlap");
            this.objectWriter.writeSpatialOverlap(trestleObject, intersectedObject, overlap);
        });

//                    Temporal relations
        final Completable temporalCompletable = writeTemporalRelationships(trestleObject, intersectedObject, temporalComparisonReport, trestleTransaction);

        return Completable.mergeArray(relationsCompletable, spatialComparisonCompletable, temporalCompletable)
                .andThen(Completable.defer(() -> {
                    this.relationTracker.addRelation(individualIRI, intersectedObjectID);
                    return Completable.complete();
                }));
    }

    private Completable writeTemporalRelationships(T trestleObject, T intersectedObject, TemporalComparisonReport temporalComparisonReport, TrestleTransaction transaction) {
        return Observable.fromIterable(temporalComparisonReport.getRelations())
                .flatMapCompletable(tRelation -> {
                    logger.debug("Writing temporal relationship {}", tRelation);
                    final TrestleTransaction tt = this.ontology.createandOpenNewTransaction(transaction);
                    return this.objectWriter.writeObjectRelationship(trestleObject, intersectedObject, tRelation, tt)
                            .doOnComplete(() -> this.ontology.returnAndCommitTransaction(tt))
                            .doOnError(error -> this.ontology.returnAndAbortTransaction(tt));
                });
    }

    private Completable writeSpatialComparison(T trestleObject, T intersectedObject, SpatialComparisonReport spatialComparisonReport, TrestleTransaction trestleTransaction) {
        return Single.just(spatialComparisonReport.getSpatialOverlap())
                .flatMapMaybe(overlap -> {
                    if (overlap.isPresent()) {
                        return Maybe.just(overlap);
                    }
                    return Maybe.empty();
                })
                .map(Optional::get)
                .flatMapCompletable(overlap -> {
                    logger.trace("Writing spatial overlaps {}", overlap);
                    final TrestleTransaction tt = this.ontology.createandOpenNewTransaction(trestleTransaction);
                    return this.objectWriter.writeSpatialOverlap(trestleObject, intersectedObject, overlap)
                            .doOnComplete(() -> this.ontology.returnAndCommitTransaction(tt))
                            .doOnError(error -> this.ontology.returnAndAbortTransaction(tt));
                });
    }

    private TrestlePair<T, TrestlePair<SpatialComparisonReport, TemporalComparisonReport>> buildTrestlePair(IRI individualIRI, T trestleObject, T intersectedObject) {
        logger.debug("Writing relationships between {} and {}", individualIRI, this.trestleParser.classParser.getIndividual(intersectedObject));
        final SpatialComparisonReport spatialComparisonReport = this.spatialEngine.compareTrestleObjects(trestleObject, intersectedObject, 0.9);
        final TemporalComparisonReport temporalComparisonReport = this.temporalEngine.compareObjects(trestleObject, intersectedObject);
        return new TrestlePair<>(intersectedObject, new TrestlePair<>(spatialComparisonReport, temporalComparisonReport));
    }

    private boolean shouldCompute(IRI individualIRI, T object) {
        final IRI intersectedIRI = this.trestleParser.classParser.getIndividual(object).getIRI();
//                        If we've already computed these two, don't do them again.
//                        Or, if the objects are the same, skip them
        if (this.relationTracker.hasRelation(individualIRI, intersectedIRI) || intersectedIRI.equals(individualIRI)) {
            logger.debug("Already computed relationships between {} and {}", individualIRI, intersectedIRI);
            return false;
        }
        return true;
    }
}