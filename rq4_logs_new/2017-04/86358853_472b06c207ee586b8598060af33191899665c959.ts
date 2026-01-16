import {HandleResponse, MappedParameters, HandleEvent, Execute, Respondable, HandleCommand, Respond, Instruction, Response, HandlerContext , Plan} from '@atomist/rug/operations/Handlers'
import {TreeNode, Match, PathExpression} from '@atomist/rug//tree/PathExpression'
import {EventHandler, ResponseHandler, ParseJson, CommandHandler, Secrets, MappedParameter, Parameter, Tags, Intent} from '@atomist/rug/operations/Decorators'
import {Project} from '@atomist/rug/model/Core'
import {wrap} from './Common'

@CommandHandler("CopyS3BucketToS3Bucket","Copy an S3 Bucket's contents to another S3 Bucket")
@Tags("github", "aws", "s3")
@Intent("copy s3 bucket to s3 bucket")
@Secrets("secret://team?path=aws/access_key", "secret://team?path=aws/secret_key")
class S3BucketToBucketCopy implements HandleCommand {

  @Parameter({description: "S3 source bucket name", pattern: "^.*$"})
  sourceBucket: string

  @Parameter({description: "S3 destination bucket name", pattern: "^.*$"})
  destinationBucket: string

  @Parameter({description: "AWS region", pattern: "^.*$"})
  region: string = "us-east-1"

  @MappedParameter("atomist://correlation_id")
  corrid: string

  handle(command: HandlerContext) : Plan {
    let result = new Plan()
    result.add(wrap({instruction:
              {name: "CopyS3Bucket",
              kind: "execute",
              parameters: this }}, `Successfully copied ${this.sourceBucket} to ${this.destinationBucket} in ${this.region}`,this))
    return result;
  }
}

export const searcher = new S3BucketToBucketCopy();