package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

/**
 * Created by kishore on 3/20/16.
 */
public class DumpClickListener implements OnClickListener{

    private static final String TAG = DumpClickListener.class.getName();
    private static final int TEST_CNT = 50;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private TextView mTextView;
    private ContentResolver mContentResolver;
    private Uri mUri;

    public DumpClickListener(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public void onClick(View v) {
        Cursor resultCursor = null;
        switch(v.getId()){
            case R.id.button_ldump:
                // get local partition
                resultCursor = mContentResolver.query(mUri, null,
                        "@", null, null);
                break;
            case R.id.button_gdump:
                // get the whole DHT
                resultCursor = mContentResolver.query(mUri, null,
                        "*", null, null);
                break;
            default:
                Log.e(TAG, "Unknown button id");
                break;
        }
        if (resultCursor == null) {
            mTextView.append("empty");
        } else {
            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

            // iterate through the results and add them to the text view
            while (resultCursor.moveToNext()) {
                String opMsg = "query key " + resultCursor.getString(keyIndex) + " value " + resultCursor.getString(valueIndex);
                Log.v(TAG, opMsg);
                mTextView.append(opMsg + "\n");
            }
            resultCursor.close();
        }
    }
}
