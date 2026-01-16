import { MigrationInterface, QueryRunner } from "typeorm";
import {
  EmissionFactor,
  EmissionFactorValue,
  EmissionSource,
  MeasurementUnit,
} from "../entity";
import {
  emissionFactors,
  logisticsUpdate,
  purchasesUpdate,
  wastesUpdate,
  wasteWithNewSources,
} from "../seed/data/emissionFactorUpdate1";

export class EmissionFactorUpdate1643291785198 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    // Add new emission factors and their values
    for (let ef of emissionFactors) {
      const es = await queryRunner.connection.manager.findByIds(
        EmissionSource,
        [ef.emissionSource]
      );
      const unit = await queryRunner.connection.manager.findOne(
        MeasurementUnit,
        ef.unit,
        {
          relations: ["physicalQuantity"],
        }
      );
      const newEf = await queryRunner.connection.manager
        .create(EmissionFactor, {
          name: ef.name,
          source: ef.source,
          geographicalArea: ef.region,
          emissionSources: Promise.resolve(es),
          dataSourceType: ef.sourceType,
          physicalQuantity: unit!.physicalQuantity,
          values: [],
        })
        .save();
      console.log("new EF", newEf);
      const efValue = await queryRunner.connection.manager
        .create(EmissionFactorValue, {
          value: ef.value / unit!.conversionFactor,
          startDate: ef.year,
          endDate: 2050,
          emissionFactor: newEf,
        })
        .save();
      newEf.values = newEf.values.concat(efValue);
      await newEf.save();
    }

    // Rename old emission factors
    await queryRunner.query(`
    UPDATE "public"."emission_factor" SET "name"='Taksikulut' WHERE "name"='Taksimatkat';
    UPDATE "public"."emission_factor" SET "name"='Laivaliput' WHERE "name"='Laivamatkat';
    UPDATE "public"."emission_factor" SET "name"='Lentoliput' WHERE "name"='Lentomatkat';
    UPDATE "public"."emission_factor" SET "name"='Valmismatkahankinnat' WHERE "name"='Valmismatkat';
    UPDATE "public"."emission_factor" SET "name"='Junaliput' WHERE "name"='Junamatkat';
    `);

    // Update emission factor values
    for (let waste of wastesUpdate) {
      await queryRunner.query(`
      with t as (
        select ev.id, ef.name as name, ev.value as value, ev."startDate" as startDate, ev."endDate" as endDate 
        from emission_factor_value as ev
        join emission_factor as ef on ev."emissionFactorId" = ef.id
        where ef.name = '${waste.name}' and ev."startDate" = 2011
      )
      update emission_factor_value
      set value = ${waste.value}
      from t
      where t.id = emission_factor_value.id;`);
    }

    for (let purchase of purchasesUpdate) {
      await queryRunner.query(`
      with t as (
        select ev.id, ef.name as name, ev.value as value, ev."startDate" as startDate, ev."endDate" as endDate 
        from emission_factor_value as ev
        join emission_factor as ef on ev."emissionFactorId" = ef.id
        where ef.name = '${purchase.name}' and ev."startDate" = 2020
      )
      update emission_factor_value
      set value = ${purchase.value}
      from t
      where t.id = emission_factor_value.id;`);
    }
    for (let waste of wasteWithNewSources) {
      await queryRunner.query(`
      with t as (
        select ev.id, ef.name as name, ev.value as value, ev."startDate" as startDate, ev."endDate" as endDate 
        from emission_factor_value as ev
        join emission_factor as ef on ev."emissionFactorId" = ef.id
        where ef.name = '${waste.name}' and ev."startDate" = 2011
      )
      update emission_factor_value
      set value = ${waste.value}
      from t
      where t.id = emission_factor_value.id;`);

      await queryRunner.query(`
      update emission_factor
      set source = '${waste.source}'
      where name = '${waste.name}';
      `);
    }
    for (let l of logisticsUpdate) {
      await queryRunner.query(`
      with t as (
        select ev.id, ef.name as name, ev.value as value, ev."startDate" as startDate, ev."endDate" as endDate 
        from emission_factor_value as ev
        join emission_factor as ef on ev."emissionFactorId" = ef.id
        where ef.name = '${l.name}' and ev."startDate" = 2016
      )
      update emission_factor_value
      set value = ${l.value}
      from t
      where t.id = emission_factor_value.id;`);
    }
    console.log("done updating emission factors");
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.connection.manager.connection.close();
  }
}