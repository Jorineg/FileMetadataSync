-- RPC function to get storage object ID (needed for REST API backend)
-- Run this on your Supabase database once

CREATE OR REPLACE FUNCTION get_storage_object_id(p_bucket_id TEXT, p_object_name TEXT)
RETURNS UUID
LANGUAGE sql
SECURITY DEFINER
AS $$
    SELECT id FROM storage.objects
    WHERE bucket_id = p_bucket_id AND name = p_object_name
    LIMIT 1;
$$;

-- Grant execute to service role
GRANT EXECUTE ON FUNCTION get_storage_object_id TO service_role;

