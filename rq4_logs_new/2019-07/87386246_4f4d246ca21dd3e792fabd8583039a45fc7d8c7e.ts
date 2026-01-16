import { TestBed } from '@angular/core/testing';
import { Http, Response, ResponseOptions, ResponseType } from '@angular/http';
import { ActionItem } from '../../../domain/action-item';
import { ConfigService } from '../../config.service';
import { JenkinsService } from './jenkins.service';
import { Observable } from 'rxjs/Rx';
import { GithubConfig } from '../../../domain/github-config';
import { FakeConfigService } from '../../../testing/fake-config.service';
import { TEST_GITHUB_CONFIG, TEST_VSTS_CONFIG } from '../../../testing/constants';
import { VstsConfig } from '../../../domain/vsts-config';

export type Spy<T> = { [Method in keyof T]: jasmine.Spy; };

describe('JenkinsService', () => {
  let serviceUnderTest: JenkinsService;
  let http: Spy<Http>;
  let configService: FakeConfigService;

  beforeEach(() => {
    http = jasmine.createSpyObj('Http', ['get']);
    TestBed.configureTestingModule({
      providers: [
        JenkinsService,
        {provide: GithubConfig, useValue: TEST_GITHUB_CONFIG},
        {provide: VstsConfig, useValue: TEST_VSTS_CONFIG},
        {provide: ConfigService, useClass: FakeConfigService},
        {provide: Http, useValue: http}
      ]
    });
    serviceUnderTest = TestBed.get(JenkinsService);
    configService = TestBed.get(ConfigService);
  });

  it('should return luminate online builds that have failed', async (done: DoneFn) => {
    const opts = {type: ResponseType.Default, status: 200, body: getTestJenkinsData()};
    const responseOpts = new ResponseOptions(opts);
    http.get.and.returnValues(Observable.of(), Observable.of(), Observable.of(new Response(responseOpts)));

    configService.repos = {'luminate-online': 'luminate-online'};

    const actionItems: ActionItem[] = await serviceUnderTest.getActionItems();
    expect(actionItems.length).toEqual(3);
    done();
  });
});

function getTestJenkinsData() {
  return {
    'jobs': [{
      'name': 'bbcs-sdk',
      'color': 'red',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 88959, 'number': 13, 'timestamp': 1508379068555 },
      'lastCompletedBuild': {
        'number': 13,
        'result': 'FAILURE',
        'timestamp': 1508379068555,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/bbcs-sdk/13/'
      }
    }, {
      'name': 'bbsp-sdk',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 49003, 'number': 30, 'timestamp': 1520433068274 },
      'lastCompletedBuild': {
        'number': 30,
        'result': 'SUCCESS',
        'timestamp': 1520433068274,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/bbsp-sdk/30/'
      }
    }, {
      'name': 'common-test-legacy',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 79988, 'number': 4, 'timestamp': 1561565287500 },
      'lastCompletedBuild': {
        'number': 4,
        'result': 'SUCCESS',
        'timestamp': 1561565287500,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/common-test-legacy/4/'
      }
    }, {
      'name': 'create-release-branches',
      'color': 'blue',
      'inQueue': true,
      'lastBuild': { 'estimatedDuration': 22975, 'number': 31345, 'timestamp': 1561657440023 },
      'lastCompletedBuild': {
        'number': 31345,
        'result': 'SUCCESS',
        'timestamp': 1561657440023,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/create-release-branches/31345/'
      }
    }, {
      'name': 'HTTPClient',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 15518, 'number': 15, 'timestamp': 1458757302105 },
      'lastCompletedBuild': {
        'number': 15,
        'result': 'SUCCESS',
        'timestamp': 1458757302105,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/HTTPClient/15/'
      }
    }, {
      'name': 'knowwho-api-wsdl',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 34537, 'number': 7, 'timestamp': 1481312808761 },
      'lastCompletedBuild': {
        'number': 7,
        'result': 'SUCCESS',
        'timestamp': 1481312808761,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/knowwho-api-wsdl/7/'
      }
    }, {
      'name': 'knowwho-sdk',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 69696, 'number': 40, 'timestamp': 1499977242648 },
      'lastCompletedBuild': {
        'number': 40,
        'result': 'SUCCESS',
        'timestamp': 1499977242648,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/knowwho-sdk/40/'
      }
    }, {
      'name': 'luminate-iats-client',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 49494, 'number': 2, 'timestamp': 1561655325020 },
      'lastCompletedBuild': {
        'number': 2,
        'result': 'SUCCESS',
        'timestamp': 1561655325020,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/luminate-iats-client/2/'
      }
    }, {
      'name': 'luminate-online-componentTests-develop',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 1659217, 'number': 898, 'timestamp': 1561617554300 },
      'lastCompletedBuild': {
        'number': 898,
        'result': 'SUCCESS',
        'timestamp': 1561617554300,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/luminate-online-componentTests-develop/898/'
      }
    }, {
      'name': 'luminate-online-componentTests-release-19.5.1',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 2051831, 'number': 1, 'timestamp': 1561594245670 },
      'lastCompletedBuild': {
        'number': 1,
        'result': 'SUCCESS',
        'timestamp': 1561594245670,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/luminate-online-componentTests-release-19.5.1/1/'
      }
    }, {
      'name': 'luminate-online-componentTests-template',
      'color': 'disabled',
      'inQueue': false,
      'lastBuild': null,
      'lastCompletedBuild': null
    }, {
      'name': 'luminate-online-customer-tree-kit',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 475386, 'number': 204, 'timestamp': 1560213707177 },
      'lastCompletedBuild': {
        'number': 204,
        'result': 'SUCCESS',
        'timestamp': 1560213707177,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/luminate-online-customer-tree-kit/204/'
      }
    }, {
      'name': 'luminate-online-junits-develop',
      'color': 'yellow',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 8272803, 'number': 831, 'timestamp': 1561609337412 },
      'lastCompletedBuild': {
        'number': 831,
        'result': 'UNSTABLE',
        'timestamp': 1561609337412,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/luminate-online-junits-develop/831/'
      }
    }, {
      'name': 'luminate-online-junits-release-19.5.1',
      'color': 'yellow',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 8221322, 'number': 1, 'timestamp': 1561596297511 },
      'lastCompletedBuild': {
        'number': 1,
        'result': 'UNSTABLE',
        'timestamp': 1561596297511,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/luminate-online-junits-release-19.5.1/1/'
      }
    }, {
      'name': 'luminate-online-junits-template',
      'color': 'disabled',
      'inQueue': false,
      'lastBuild': null,
      'lastCompletedBuild': null
    }, {
      'name': 'luminate-online-main-develop',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 2180491, 'number': 942, 'timestamp': 1561607263463 },
      'lastCompletedBuild': {
        'number': 942,
        'result': 'SUCCESS',
        'timestamp': 1561607263463,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/luminate-online-main-develop/942/'
      }
    }, {
      'name': 'luminate-online-main-release-19.5.1',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 1772726, 'number': 2, 'timestamp': 1561592464604 },
      'lastCompletedBuild': {
        'number': 2,
        'result': 'SUCCESS',
        'timestamp': 1561592464604,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/luminate-online-main-release-19.5.1/2/'
      }
    }, {
      'name': 'luminate-online-main-template',
      'color': 'disabled',
      'inQueue': false,
      'lastBuild': null,
      'lastCompletedBuild': null
    }, {
      'name': 'luminate-online-official-jgitflow-release',
      'color': 'red',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 2042539, 'number': 85, 'timestamp': 1559771554168 },
      'lastCompletedBuild': {
        'number': 85,
        'result': 'FAILURE',
        'timestamp': 1559771554168,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/luminate-online-official-jgitflow-release/85/'
      }
    }, {
      'name': 'next-gen-integration',
      'color': 'blue',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 63191, 'number': 53, 'timestamp': 1532715128567 },
      'lastCompletedBuild': {
        'number': 53,
        'result': 'SUCCESS',
        'timestamp': 1532715128567,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/next-gen-integration/53/'
      }
    }, {
      'name': 'sleep-job',
      'color': 'aborted',
      'inQueue': false,
      'lastBuild': { 'estimatedDuration': 692923, 'number': 6, 'timestamp': 1515619976605 },
      'lastCompletedBuild': {
        'number': 6,
        'result': 'ABORTED',
        'timestamp': 1515619976605,
        'url': 'https://jenkins-releases.blackbaudcloud.com/job/sleep-job/6/'
      }
    }]
  };
}