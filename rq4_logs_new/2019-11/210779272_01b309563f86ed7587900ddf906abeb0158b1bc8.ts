import sequelize, { CreateOptions, FindOptions } from 'sequelize';

import { IBusinessEntity } from '../declarations';

import Businesses from './sequelize/Businesses';
import Internships from './sequelize/Internships';

import { paginate, PaginateOpts } from '../api/helpers/pagination.helper';
import { extractCount } from './helpers/options';
import { PaginateList } from './helpers/type';
import { checkPartialBusiness, checkPartialInternship } from './helpers/check';

/** @interface BusinessOpts Interface of all availables filters for businesses list */
export interface BusinessOpts {
    /** @property {string[]} countries List of selected countries to filter */
    countries?: string[];

    /** @property {string} name Part of name to apply filter on name field (database) */
    name?: string;
}

/**
 * @class BusinessModelStruct
 *
 * API for business use in database
 */
class BusinessModelStruct {
    /**
     * @summary Method used to retrieve businesses
     * @param {BusinessOpts} businessOpts business find options
     * @param {PaginateOpts} pageOpts pagination options
     * @returns {Promise<PaginateList<IBusinessEntity>>} Resolve: Paginated IBusinessEntity
     * @returns {Promise<void>} Resolve: void if nothing is found
     * @returns {Promise<any>} Reject: database error
     */
    public getBusinesses(
        businessOpts: BusinessOpts,
        pageOpts: PaginateOpts,
    ): Promise<PaginateList<IBusinessEntity>> {
        return new Promise((resolve, reject) => {
            const opts = this._buildFindOpts(businessOpts);

            let max: number;
            Businesses.count(extractCount(opts))
                .then((rowNbr) => {
                    max = rowNbr;
                    return Businesses.findAll(paginate(pageOpts, opts));
                })
                .then(async (businesses: any) =>
                    businesses.length
                        ? resolve({
                              page: pageOpts.page,
                              data: businesses as IBusinessEntity[],
                              length: businesses.length,
                              max,
                          })
                        : resolve(),
                )
                .catch((e) => reject(e));
        });
    }

    /**
     * @summary Method used to create a new business
     * @notice If business already exist, update it
     * @notice Also create or link sub-models if given
     * @param {IBusinessEntity} business New business data
     * @returns {Promise<IBusinessEntity>} Resolve: IBusinessEntity
     * @returns {Promise<any>} Reject: database error
     */
    public createBusiness(business: IBusinessEntity): Promise<IBusinessEntity> {
        return new Promise(async (resolve, reject) => {
            try {
                // If id is given, check if business already exist
                // If so, launch update on it
                if (business.id) {
                    const prev = await Businesses.findByPk(business.id);
                    if (prev) {
                        const updated = await this.updateBusiness(business.id, business);
                        resolve(updated);
                        return;
                    }
                }

                // Else, create new business
                const created = await Businesses.create(business, this._buildCreateOpts(business));

                // TODO: Add socket chanel + emit create
                // TODO: Add to stats
                resolve(created.toJSON() as IBusinessEntity);
            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * @summary Method used to retrieve a business by his identifier
     * @notice Include internships by default
     * @param {number} id Business identifier
     * @returns {Promise<IBusinessEntity>} Resolve: IBusinessEntity
     * @returns {Promise<void>} Resolve: void if nothing is found
     * @returns {Promise<any>} Reject: database error
     */
    public getBusiness(id: number): Promise<IBusinessEntity> {
        return new Promise((resolve, reject) => {
            Businesses.findByPk(id, { include: [{ model: Internships, as: 'internships' }] })
                .then((val) => resolve(val ? (val.toJSON() as IBusinessEntity) : undefined))
                .catch((e) => reject(e));
        });
    }

    /**
     * @summary Method used to update a business entity
     * @notice If not any business is found, but all update data required to create
     * a new business is available, create this new business
     * @param {number} id business identifier
     * @param {Partial<IBusinessEntity>} next business data to update
     * @returns {Promise<IBusinessEntity>} Resolve: IBusinessEntity
     * @returns {Promise<void>} Resolve: void if nothing is found
     * @returns {Promise<any>} Reject: database error
     */
    public updateBusiness(id: number, next: Partial<IBusinessEntity>): Promise<IBusinessEntity> {
        return new Promise(async (resolve, reject) => {
            try {
                const business = await Businesses.findByPk(id);
                if (!business) {
                    // Try to create business if all required fields are given
                    if (checkPartialBusiness(next)) {
                        delete next.id; // Prevent recursive call by removing id;
                        const created = await this.createBusiness(next);
                        return resolve(created);
                    }
                    return resolve();
                }

                if (next.name) {
                    business.set('name', next.name);
                }
                if (next.country) {
                    business.set('country', next.country);
                }
                if (next.city) {
                    business.set('city', next.city);
                }
                if (next.postalCode) {
                    business.set('postalCode', next.postalCode);
                }
                if (next.address) {
                    business.set('address', next.address);
                }
                if (next.additional) {
                    business.set('additional', next.additional);
                }

                const updated = await business.save();

                // TODO: Also update internships

                // TODO: Add socket edit + emit
                // TODO: Update counter for this update;

                resolve(updated.toJSON() as IBusinessEntity);
            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * @summary Method used to remove business
     * @param {number} id business identifier
     * @returns {Promise<void>} Resolve: OK (may not be deleted, just resolve)
     * @returns {Promise<any>} Reject: database error
     */
    public removeBusiness(id: number): Promise<void> {
        // TODO: Setup archives
        // TODO: Add archives function for internships on availables modes

        // TODO: Add socket remove + emit;
        // TODO: Update counter for this remove;
        return new Promise((resolve, reject) => {
            Businesses.findByPk(id!)
                .then((val) => (val ? val.destroy() : undefined))
                .then(() => resolve())
                .catch((e) => reject(e));
        });
    }

    /**
     * @summary Method used to link business and internship
     * @param {number} businessId business identifier
     * @param {number} internshipId internship identifier
     * @returns {Promise<IBusinessEntity>} Resolve: linked entity
     * @returns {Promise<void>} Resolve: nothing found to link
     * @returns {Promise<any>} Reject: database error
     */
    public linkToInternship(businessId: number, internshipId: number): Promise<IBusinessEntity> {
        return new Promise(async (resolve, reject) => {
            try {
                const business = await Businesses.findByPk(businessId);
                if (!business) {
                    return resolve();
                }
                const internship = await Internships.findByPk(internshipId);
                if (!internship) {
                    return resolve();
                }

                await business.addInternship(internship);
                return resolve(business.toJSON() as IBusinessEntity);
            } catch (error) {
                reject(error);
            }
        });
    }

    private _buildFindOpts(opts: BusinessOpts): FindOptions {
        const tmp: FindOptions = {
            attributes: {
                include: [
                    [sequelize.fn('count', sequelize.col(`internships.businessId`)), 'count'],
                ],
            },
            include: [
                {
                    model: Internships,
                    as: 'internships',
                    attributes: [],
                    duplicating: false,
                },
            ],
            where: {},
            group: [sequelize.col(`Businesses.id`)],
        };

        if (opts.countries) {
            // If country list is given, add it to query
            // Sequelize will translate it by "country in countries"
            (tmp.where as any).country = opts.countries;
        }

        if (opts.name) {
            // If name filter is given, apply it using substring
            (tmp.where as any).name = { [sequelize.Op.substring]: opts.name };
        }

        return tmp;
    }

    private _buildCreateOpts(campaign: IBusinessEntity): CreateOptions {
        const opts: CreateOptions = { include: [] };

        if (campaign.internships) {
            let set = true;
            for (const internship of campaign.internships) {
                if (!checkPartialInternship(internship)) {
                    set = false;
                    break;
                }
            }
            if (set) {
                opts.include.push({ association: Businesses.associations.internships });
            }
        }

        if (opts.include.length === 0) {
            delete opts.include;
        }

        return opts;
    }
}

// Init Struct and export as default this model
const BusinessModel = new BusinessModelStruct();
export default BusinessModel;