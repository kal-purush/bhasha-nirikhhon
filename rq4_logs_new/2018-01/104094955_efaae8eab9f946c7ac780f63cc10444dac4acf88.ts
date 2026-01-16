import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute, Params } from '@angular/router';
import { Location } from '@angular/common';
import { MatDialog, MatDialogRef, MatDialogConfig, MatSnackBar, MatRadioChange } from '@angular/material';
import { SoftwareComponent } from '../software/software.component';
import { StixService } from '../../../stix.service';
import { Malware, AttackPattern, Indicator, IntrusionSet, CourseOfAction, Filter, ExternalReference, Relationship } from '../../../../models';
import { Constance } from '../../../../utils/constance';

@Component({
  selector: 'software-set-edit',
  templateUrl: './software-edit.component.html',
  styleUrls: ['software-edit.component.scss']
})
export class SoftwareEditComponent extends SoftwareComponent implements OnInit {
    public attackPatterns: AttackPattern[] = [];
    public indicators: Indicator[] = [];
    public courseOfActions: CourseOfAction[] = [];
    public intrusionSets: IntrusionSet[] = [];
    public newRelationships: Relationship[] = [];
    public savedRelationships: Relationship[] = [];
    public deletedRelationships: Relationship[] = [];
    public allCitations: any = [];
    public contributors: string[] = [];
    public createNewOnly: boolean = true;
    public mitreId: any;
    public softwareTypes: string[] = ['Malware', 'Tool/Utility'];
    public softwareType: string = 'Malware';
    public origType: string = 'Malware';

   constructor(
        public stixService: StixService,
        public route: ActivatedRoute,
        public router: Router,
        public dialog: MatDialog,
        public location: Location,
        public snackBar: MatSnackBar) {
        super(stixService, route, router, dialog, location, snackBar);
    }

    public ngOnInit() {
        let testRoute: any;
        testRoute = this.route;
        if (testRoute.url.value[1].path.match(/^tool/)) {
            this.stixService.url = Constance.TOOL_URL;
            this.softwareType = 'Tool/Utility';
            this.origType = 'Tool/Utility';
        }
        const subscription =  super.get().subscribe(
            (data) => {
                this.malware = data;
                this.malware.attributes.external_references.reverse();
                console.log(this.malware);
                this.getTechniques(false);
                this.getAllAliases();
                this.getCitationsAndContributors();
                this.assignCitations();
            }, (error) => {
                // handle errors here
                 console.log('error ' + error);
            }, () => {
                // prevent memory links
                if (subscription) {
                    subscription.unsubscribe();
                }
            }
        );
    }

    public typeChange(event: MatRadioChange) {
        this.softwareType = event.value;
        if (this.softwareType === 'Malware') {
            this.stixService.url = Constance.MALWARE_URL;
        }
        else {
            this.stixService.url = Constance.TOOL_URL;
        }
        console.log(this.softwareType);
    }

    public getNewCitation(refToAdd) {
        this.allCitations.push(refToAdd);
        this.allCitations = this.allCitations.sort((a, b) => a.source_name.toLowerCase() < b.source_name.toLowerCase() ? -1 : a.source_name.toLowerCase() > b.source_name.toLowerCase() ? 1 : 0);
        this.allCitations = this.allCitations.filter((citation, index, self) => self.findIndex((t) => t.source_name === citation.source_name) === index);
    }

    public addContributor(): void {
        if (!('x_mitre_contributors' in this.malware.attributes)) {
            this.malware.attributes.x_mitre_contributors = [];
        }
        let contributorName = '';
        this.malware.attributes.x_mitre_contributors.unshift(contributorName);
    }

    public removeContributor(contributor): void {
        this.malware.attributes.x_mitre_contributors = this.malware.attributes.x_mitre_contributors.filter((h) => h !== contributor);
    }

    public assignCitations(): void {
        for (let i in this.malware.attributes.external_references) {
            this.malware.attributes.external_references[i].citeButton = 'Generate Citation Text';
            this.malware.attributes.external_references[i].citation = '[[Citation: ' + this.malware.attributes.external_references[i].source_name + ']]';
            this.malware.attributes.external_references[i].citeref = '[[CiteRef::' + this.malware.attributes.external_references[i].source_name + ']]';
        }
    }

    public getCitationsAndContributors(): void {
        let uri = Constance.MULTIPLES_URL;
        let subscription =  super.getByUrl(uri).subscribe(
            (data) => {
                let extRefs = [];
                for (let currObj of data) {
                    if (currObj.attributes.external_references && currObj.attributes.external_references.source_name !== 'mitre-attack') {
                        extRefs = extRefs.concat(currObj.attributes.external_references);
                    }
                    this.contributors = this.contributors.concat(currObj.attributes.x_mitre_contributors);
                }
                let configUri = Constance.CONFIG_URL;
                let subscription =  super.getByUrl(configUri).subscribe(
                    (res) => {
                        if (res && res.length) {
                            for (let currRes of res) {
                                if (currRes.attributes.configKey === 'references') {
                                  extRefs = extRefs.concat(currRes.attributes.configValue);
                                }
                            }
                        }
                        extRefs = extRefs.sort((a, b) => a.source_name.toLowerCase() < b.source_name.toLowerCase() ? -1 : a.source_name.toLowerCase() > b.source_name.toLowerCase() ? 1 : 0);
                        this.allCitations = extRefs.filter((citation, index, self) => self.findIndex((t) => t.source_name === citation.source_name) === index);
                    }, (error) => {
                        // handle errors here
                         console.log('error ' + error);
                    }, () => {
                        // prevent memory links
                        if (subscription) {
                            subscription.unsubscribe();
                        }
                    }
                );
                this.contributors = this.contributors.filter((elem, index, self) => self.findIndex((t) => t === elem) === index).sort().filter(Boolean);
            }, (error) => {
                // handle errors here
                 console.log('error ' + error);
            }, () => {
                // prevent memory links
                if (subscription) {
                    subscription.unsubscribe();
                }
            }
        );
    }

    public addAliasesToMalware(): void {
        this.malware.attributes.x_mitre_aliases = [];
        this.malware.attributes.x_mitre_aliases.push(this.malware.attributes.name);
        if (this.aliases.length > 0) {
            for (let alias of this.aliases){
                if (alias.name !== ''){
                    this.malware.attributes.x_mitre_aliases.push(alias.name);
                    if (alias.description !== '') {
                        let extRef = new ExternalReference();
                        extRef.source_name = alias.name;
                        extRef.description = alias.description;
                        this.malware.attributes.external_references.push(extRef);
                    }
                }
            }
        }
        console.log(this.malware.attributes.x_mitre_aliases);
    }

    public createRelationships(id: string): void {
        for (let technique of this.addedTechniques) {
            let currTechnique = this.techniques.filter((h) => h.name === technique.name);
            if (currTechnique.length > 0) {
                this.saveRelationship(id, currTechnique[0].id, technique.description, technique.relationship);
            }
            this.origRels = this.origRels.filter((h) => h.id !== technique.relationship);
        }
    }

    public saveRelationship(source_ref: string, target_ref: string, description: string, id: string): void {
        let relationship = new Relationship();
        relationship.attributes.source_ref = source_ref;
        relationship.attributes.target_ref = target_ref;
        if (description !== '') {
            relationship.attributes.external_references = [];
            relationship.attributes.description = description;
            let citationArr = super.matchCitations(relationship.attributes.description);
            for (let name of citationArr) {
                let citation = this.allCitations.find((p) => p.source_name === name);
                if (citation !== undefined) {
                    relationship.attributes.external_references.push(citation);
                }
            }
        }
        relationship.attributes.relationship_type = 'uses';
        if (id !== '') {
            relationship.id = id;
            console.log(relationship);
            this.stixService.url = Constance.RELATIONSHIPS_URL;
            let subscription = super.save(relationship).subscribe(
                (data) => {
                    console.log(data);
                }, (error) => {
                    // handle errors here
                    console.log('error ' + error);
                }, () => {
                    // prevent memory links
                    if (subscription) {
                        subscription.unsubscribe();
                    }
                }
            );
        } else {
            let subscription = super.create(relationship).subscribe(
                (data) => {
                    console.log(data);
                }, (error) => {
                    // handle errors here
                    console.log('error ' + error);
                }, () => {
                    // prevent memory links
                    if (subscription) {
                        subscription.unsubscribe();
                    }
                }
            );
        }
    }

    public removeRelationships(id: string): void {
        for (let rel of this.origRels) {
            rel.url = Constance.RELATIONSHIPS_URL;
            rel.id = rel.attributes.id;
            this.delete(rel).subscribe(
                () => {
                }
            );
        }
    }

    public removeContributors(): void {
        if ('x_mitre_contributors' in this.malware.attributes) {
            this.removeContributor("");
            if (this.malware.attributes.x_mitre_contributors.length === 0) {
                delete this.malware.attributes['x_mitre_contributors'];
            }
        }
    }

    public getMitreId(): void {
        for (let i in this.malware.attributes.external_references) {
            if (this.malware.attributes.external_references[i].external_id !== undefined) {
                this.mitreId = Object.assign({}, this.malware.attributes.external_references[i]);
            }
        }
    }

    public addExtRefs(): void {
        let citationArr = super.matchCitations(this.malware.attributes.description);
        if (this.mitreId !== undefined) {
            this.malware.attributes.external_references.push(this.mitreId);
        }
        console.log(citationArr);
        console.log(this.allCitations);
        for (let name of citationArr) {
            let citation = this.allCitations.find((p) => p.source_name === name);
            console.log(citation);
            if (citation !== undefined) {
                this.malware.attributes.external_references.push(citation);
            }
        }
    }

    public saveMalware(): void {
        this.getMitreId();
        this.malware.attributes.external_references = [];
        this.addExtRefs();
        this.addAliasesToMalware();
        this.removeContributors();
        this.malware.attributes.external_references.reverse();
        let sub = super.saveButtonClicked().subscribe(
            (data) => {
                this.location.back();
                this.createRelationships(data.id);
                this.removeRelationships(data.id);
            }, (error) => {
                // handle errors here
                console.log('error ' + error);
            }, () => {
                // prevent memory links
                if (sub) {
                    sub.unsubscribe();
                }
            }
        );
    }

    public addTechnique(): void {
        let currTechnique = {};
        currTechnique['name'] = '';
        currTechnique['description'] = '';
        currTechnique['relationship'] = '';
        this.addedTechniques.unshift(currTechnique);
        this.currTechniques.unshift(this.techniques);
        for (let i in this.addedTechniques) {
            this.currTechniques[0] = this.currTechniques[0].filter((h) => h.name !== this.addedTechniques[i].name);
        }
        console.log(this.currTechniques);
    }

    public removeTechnique(technique: string, i: number): void {
        this.addedTechniques = this.addedTechniques.filter((h) => h.name !== technique);
        this.currTechniques.splice(i, 1);
        for (let index in this.currTechniques) {
            this.currTechniques[index] = this.techniques;
            for (let j in this.addedTechniques) {
                if (j !== index) {
                    this.currTechniques[index] = this.currTechniques[index].filter((h) => h.name !== this.addedTechniques[j].name)
                }
            }
        }
    }

    public checkAddedTechniques(): void {
        for (let index in this.currTechniques) {
            this.currTechniques[index] = this.techniques;
            for (let i in this.addedTechniques) {
                if (i !== index) {
                    this.currTechniques[index] = this.currTechniques[index].filter((h) => h.name !== this.addedTechniques[i].name)
                }
            }
        }
        console.log(this.currTechniques);
    }

    public found(list: any[], object: any): any {
        return list.find( (entry) => { return entry.id === object.id; } );
    }

    public addAlias(): void {
        let alias = {};
        alias['name'] = '';
        alias['description'] = '';
        this.aliases.unshift(alias);
    }

    public removeAlias(alias): void {
        this.aliases = this.aliases.filter((h) => h.name !== alias);
    }

    public filterOptions(stringToMatch: string, listToParse: any): void {
        if (stringToMatch) {
            let filterVal = stringToMatch.toLowerCase();
            return listToParse.filter((h) => h.toLowerCase().startsWith(filterVal));
        }
        return listToParse;
    }

    public loadObject(url: string, id: string, list: any ): void {
        const uri = `${url}/${id}`;
        let sub = super.getByUrl(uri).subscribe(
            (data) => {
                list.push(data);
            }, (error) => {
                console.log(error);
            }, () => {
                if (sub) {
                    sub.unsubscribe();
                }
            }
        );
    }
}