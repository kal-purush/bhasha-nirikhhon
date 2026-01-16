//package com.group4sweng.scranplan;
//
//import android.util.Log;
//
//import androidx.annotation.NonNull;
//
//import com.google.android.gms.tasks.OnCompleteListener;
//import com.google.android.gms.tasks.Task;
//import com.google.firebase.firestore.CollectionReference;
//import com.google.firebase.firestore.DocumentReference;
//import com.google.firebase.firestore.DocumentSnapshot;
//import com.google.firebase.firestore.FieldValue;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//
//public class test {
//
//    private void test(){
//        CollectionReference ref = mDatabase.collection("posts");
//        // Saving the comment as a new document
//        HashMap<String, Object> map = new HashMap<>();
//        HashMap<String, Object> extras = new HashMap<>();
//        extras.put("comments", 0);
//        extras.put("likes", 0);
//        map.put("author", mUser.getUID());
//        map.put("body", "I just created this new recipe!");
//        map.put("timestamp", FieldValue.serverTimestamp());
//        map.put("isPic", false);
//        map.put("isRecipe",true);
//        map.put("isReview", false);
//        map.put("recipeID", recipeID);
//        map.put("recipeImageURL", attachedRecipeURL);
//        map.put("recipeTitle", mAttachedRecipeTitle.getText());
//        map.put("recipeDescription", mAttachedRecipeInfo.getText());
//        ref.add(extras).addOnCompleteListener(new OnCompleteListener<DocumentReference>() {
//            @Override
//            public void onComplete(@NonNull Task<DocumentReference> task) {
//                if(task.isSuccessful()){
//                    final String docID = task.getResult().getId();
//                    map.put("docID", docID);
//                    mDatabase.collection("followers").document(mUser.getUID()).get().addOnCompleteListener(new OnCompleteListener<DocumentSnapshot>() {
//                        @Override
//                        public void onComplete(@NonNull Task<DocumentSnapshot> task) {
//                            if(task.isSuccessful()){
//                                if(task.getResult().exists()){
//                                    // Add post to followers map
//                                    DocumentSnapshot doc = task.getResult();
//                                    String space = "map" + (String) doc.get("space3");
//                                    doc.getReference().update(space, map,
//                                            "space1", doc.get("space3"),
//                                            "space2", doc.get("space1"),
//                                            "space3", doc.get("space2"),
//                                            "lastPost", map.get("timestamp")).addOnCompleteListener(new OnCompleteListener<Void>() {
//                                        @Override
//                                        public void onComplete(@NonNull Task<Void> task) {
//                                            mDatabase.collection("users").document(mUser.getUID()).update("posts", FieldValue.increment(1), "livePosts", FieldValue.increment(1)).addOnCompleteListener(new OnCompleteListener<Void>() {
//                                                                                                                                                                                                            @Override
//                                                                                                                                                                                                            public void onComplete(@NonNull Task<Void> task) {
//                                                                                                                                                                                                                mUser.setPosts(mUser.getPosts()+1);
//                                                                                                                                                                                                            }
//                                                                                                                                                                                                        }
//                                            );
//                                        }
//                                    });
//
//                                }else{
//                                    //create new followers map
//                                    HashMap<String, Object> newDoc = new HashMap<>();
//                                    ArrayList<String> arrayList = new ArrayList<>();
//                                    arrayList.add(mUser.getUID());
//                                    newDoc.put("mapA", map);
//                                    newDoc.put("mapB", (HashMap) null);
//                                    newDoc.put("mapC", (HashMap) null);
//                                    newDoc.put("space1", "A");
//                                    newDoc.put("space2", "B");
//                                    newDoc.put("space3", "C");
//                                    newDoc.put("lastPost", map.get("timestamp"));
//                                    newDoc.put("author", mUser.getUID());
//                                    newDoc.put("users", arrayList);
//                                    mDatabase.collection("followers").document(mUser.getUID()).set(newDoc).addOnCompleteListener(new OnCompleteListener<Void>() {
//                                        @Override
//                                        public void onComplete(@NonNull Task<Void> task) {
//                                            mDatabase.collection("users").document(mUser.getUID()).update("posts", FieldValue.increment(1), "livePosts", FieldValue.increment(1)).addOnCompleteListener(new OnCompleteListener<Void>() {
//                                                                                                                                                                                                            @Override
//                                                                                                                                                                                                            public void onComplete(@NonNull Task<Void> task) {
//                                                                                                                                                                                                                mUser.setPosts(mUser.getPosts()+1);
//                                                                                                                                                                                                            }
//                                                                                                                                                                                                        }
//                                            );
//                                        }
//                                    });
//                                }
//                            }
//                        }
//                    });
//                }
//            }
//        });
//    }
//
//
//
//
//
//}