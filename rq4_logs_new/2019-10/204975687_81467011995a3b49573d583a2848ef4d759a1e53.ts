import { Component } from '@angular/core';
import * as YAML from 'yaml';

import { BlockService, NotificationService } from '@/core/services';
import { YamlService } from './yaml.service';
import { Yaml } from './yaml.interface';

@Component({
  selector: 'page-yaml',
  templateUrl: './yaml.component.html',
  styleUrls: ['./yaml.component.scss'],
  providers: [YamlService],
})
export class YamlComponent {
  yaml: string;

  constructor(
    private blockService: BlockService,
    private notificationService: NotificationService,
    private yamlService: YamlService,
  ) {
    this.toYAML();
  }

  // Converts YAML string to service data
  fromYAML(): void {
    let yaml: Yaml[];
    try {
      yaml = YAML.parse(this.yaml);
    } catch (e) {
      this.notificationService.error('Parsing error');
    }
    if (yaml) {
      this.blockService.blockList = this.yamlService.yamlToBlocks(yaml);
      this.notificationService.success('Saved');
    }
  }

  // Converts service data to YAML
  toYAML(): void {
    const yamlList: Yaml[] = [{
      description: 'תסריט לניהול שיחות מצד הפונה',
      name: 'פונה',
      snippets: this.yamlService.snippetsToYaml(this.blockService.blockList),
    }];
    this.yaml = YAML.stringify(yamlList);
  }
}