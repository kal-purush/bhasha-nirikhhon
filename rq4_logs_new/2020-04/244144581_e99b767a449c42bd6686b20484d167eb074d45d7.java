package gp.lcw.sd.by.g3p.extension.dao.CommunicationSpace;


import gp.lcw.sd.by.g3p.base.domain.BaseEntity;
import gp.lcw.sd.by.g3p.extension.dao.newsMessage;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Set;

@Setter
@Getter
@Entity
@Table(name = "communicationSpace_message_Comment")
public class messageComment extends BaseEntity {

    @Column
    String commentPeopleName;//评论的人名

    @Column
    String commentContent;//评论内容

    @Column
    String messageFromUserName;//谁发的

    @Column
    String messageToUserName;//发给谁的

    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "id",insertable = false,updatable = false)
    message message;

    @OneToMany(cascade = CascadeType.ALL,mappedBy = "messageComment")
    Set<messageCommentReply> messageCommentReplySet=new HashSet<messageCommentReply>();

}