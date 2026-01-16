import { ApiProperty } from '@nestjs/swagger';
import { ContractTypes } from 'src/common/enums/contractType.enum';
import { EthnicityTypes } from 'src/common/enums/ethnicityTypes.enum';
import { GenderTypesCv } from 'src/common/enums/genderTypesCv.enum';
import { LevelType } from 'src/common/enums/levelType.enum';
import { AcademicsInformationsModel } from 'src/common/models/academicsInformation.model';
import { LanguagesInformationModel } from 'src/common/models/languagesInformation.model';
import { PreviousJobsModel } from 'src/common/models/previousJobs.model';

export class CreateOrUpdateCvDTO {
  @ApiProperty()
  candidateID: number;

  @ApiProperty()
  imageURL?: string;

  @ApiProperty()
  linkedinURL?: string;

  @ApiProperty()
  naturalness: string;

  @ApiProperty({
    description: `'1 - Mulher', '2- Mulher Trans', '3 - Homem (cis)','4 - Homem Trans', '5 - OUTROS'`,
  })
  gender: GenderTypesCv;

  @ApiProperty()
  birthDate: Date;

  @ApiProperty()
  state: string;

  @ApiProperty()
  city: string;

  @ApiProperty()
  phone?: string;

  @ApiProperty({
    description: '1- Negra, 2- Indígena, 3- Amarela, 4- Branca',
  })
  ethnicity: EthnicityTypes;

  @ApiProperty()
  isPcd: boolean;

  @ApiProperty()
  allowsWhatsapp: boolean;

  @ApiProperty({
    description: 'Área desejada',
  })
  field: string;

  @ApiProperty({
    description: '1- PJ, 2- CLT, 3-PJ OU CLT, 4- OUTROS',
  })
  contractType: ContractTypes;

  @ApiProperty({
    description: '1- Estágio, 2- JR, 3- PL, 4- SR, 5- Analista, 6 - Agente',
  })
  level: LevelType;

  @ApiProperty({
    description: 'Cargo desejavel',
  })
  role: string;

  @ApiProperty()
  academics?: AcademicsInformationsModel[];

  @ApiProperty()
  languages?: LanguagesInformationModel[];

  @ApiProperty()
  previousJobs?: PreviousJobsModel[];

  @ApiProperty()
  hardSkills: Array<string>;

  @ApiProperty()
  softSkills: Array<string>;
}