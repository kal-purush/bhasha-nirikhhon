package com.quickcanteen.quickcanteen.actions.comment;

import com.quickcanteen.quickcanteen.actions.IBaseAction;
import com.quickcanteen.quickcanteen.utils.BaseJson;
import org.json.JSONException;

import java.io.IOException;

public interface ICommentAction  extends IBaseAction {
    BaseJson postComment(int objectId,String commentContent,double rating,int isCompany) throws IOException, JSONException;

}