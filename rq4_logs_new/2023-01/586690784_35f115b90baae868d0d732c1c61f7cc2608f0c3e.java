package com.miir.totalrecall;

import com.miir.totalrecall.entity.effect.RecallEffect;

public interface RecallAccessor {
    RecallEffect.Recollection totalrecall_popRecollection();
    void totalrecall_pushRecollection(RecallEffect.Recollection r);
    RecallEffect.Recollection totalrecall_peekRecollection();
    void totalrecall_clearRecollections();
    int totalrecall_getStackLength();
}