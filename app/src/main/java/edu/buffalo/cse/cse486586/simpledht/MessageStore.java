package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;
import android.util.Log;

/**
 * Created by kishore on 3/19/16.
 */
public class MessageStore {

    private static final String TAG = MessageStore.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final ContentResolver mContentResolver;
    private final Uri mUri;

    public MessageStore(ContentResolver cr) {
        mContentResolver = cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public void insert(String key, String val) {
        ContentValues values = new ContentValues();
        values.put(KEY_FIELD, key);
        values.put(VALUE_FIELD, val);
        mContentResolver.insert(mUri, values);
        Log.v(TAG, "insert Key: " + key + " val: " + val);
    }
}
