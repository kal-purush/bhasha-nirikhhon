using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace IM.Message.Dtos;

public class HideMessageByLeaderRequestDto : IValidatableObject
{
    [Required]public string ChannelUuId { get; set; }

    [Required]public string MessageId { get; set;}
    
    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        var parse = long.TryParse(MessageId,out var messageId);
        if (!parse)
        {
            yield return new ValidationResult(
                "Invalid input.",
                new[] { "MessageId" }
            );
        }
    }
}