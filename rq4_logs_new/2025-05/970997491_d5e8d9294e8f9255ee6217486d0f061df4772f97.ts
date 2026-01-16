"use server";

import photosClient from "@/lib/photos-client";
import type { Photos } from "photos";

// --- Album Actions ---

export async function listAlbumsAction(): Promise<Photos.AlbumResponse[]> {
  try {
    const fetchedAlbums: Photos.AlbumResponse[] = [];
    // Automatically fetches more pages as needed.
    for await (const album of photosClient.albums.list()) {
      fetchedAlbums.push(album as Photos.AlbumResponse);
    }
    console.log(
      `[Action: listAlbumsAction] Success - Fetched ${fetchedAlbums.length} albums.`
    );
    return fetchedAlbums;
  } catch (error) {
    console.error("Server Action Error (listAlbumsAction):", error);
    throw new Error("Failed to list albums via server action.");
  }
}

export async function createAlbumAction(
  params: Photos.AlbumCreateParams
): Promise<Photos.AlbumResponse> {
  try {
    const newAlbum = await photosClient.albums.create(params);
    console.log(
      `[Action: createAlbumAction] Success - Created album ID: ${newAlbum.id}`
    );
    return newAlbum;
  } catch (error) {
    console.error("Server Action Error (createAlbumAction):", error);
    throw new Error(
      `Failed to create album "${params.name}" via server action.`
    );
  }
}

export async function updateAlbumAction(
  albumId: string,
  params: Photos.AlbumUpdateParams
): Promise<Photos.AlbumResponse> {
  try {
    const updatedAlbum = await photosClient.albums.update(albumId, params);
    console.log(
      `[Action: updateAlbumAction] Success - Updated album ID: ${updatedAlbum.id}`
    );
    return updatedAlbum;
  } catch (error) {
    console.error(`Server Action Error (updateAlbumAction ${albumId}):`, error);
    throw new Error(
      `Failed to update album "${params.name}" via server action.`
    );
  }
}

export async function deleteAlbumAction(albumId: string): Promise<void> {
  try {
    await photosClient.albums.delete(albumId);
    console.log(
      `[Action: deleteAlbumAction] Success - Deleted album ID: ${albumId}`
    );
  } catch (error) {
    console.error(`Server Action Error (deleteAlbumAction ${albumId}):`, error);
    throw new Error("Failed to delete album via server action.");
  }
}

// --- Asset Actions ---

export async function listAssetsAction(
  params: Photos.AssetListParams = {}
): Promise<Photos.AssetResponse[]> {
  try {
    const fetchedAssets: Photos.AssetResponse[] = [];
    // Automatically fetches more pages as needed.
    for await (const asset of photosClient.assets.list(params)) {
      fetchedAssets.push(asset as Photos.AssetResponse);
    }
    const albumInfo = params.album_id ? ` for album ${params.album_id}` : "";
    console.log(
      `[Action: listAssetsAction] Success - Fetched ${fetchedAssets.length} assets${albumInfo}.`
    );
    return fetchedAssets;
  } catch (error) {
    console.error("Server Action Error (listAssetsAction):", error);
    const albumInfo = params.album_id ? ` for album ${params.album_id}` : "";
    throw new Error(`Failed to list assets${albumInfo} via server action.`);
  }
}

// Action to add assets to an album
export async function addAssetsToAlbumAction(
  albumId: string,
  assetIds: string[]
): Promise<void> {
  try {
    await photosClient.albums.assets.add(albumId, { asset_ids: assetIds });
    console.log(
      `[Action: addAssetsToAlbumAction] Success - Added ${assetIds.length} assets to album ID: ${albumId}`
    );
  } catch (error) {
    console.error(
      `Server Action Error (addAssetsToAlbumAction ${albumId}):`,
      error
    );
    throw new Error(
      `Failed to add assets to album ${albumId} via server action.`
    );
  }
}

// Action for creating/uploading an asset
// We'll use FormData as Server Actions handle it well for file uploads
export async function createAssetAction(
  formData: FormData
): Promise<Photos.AssetResponse> {
  // Extract data from FormData
  const file = formData.get("asset_data") as File | null;
  const deviceAssetId = formData.get("device_asset_id") as string | null;
  const deviceId = formData.get("device_id") as string | null;
  const fileCreatedAt = formData.get("file_created_at") as string | null;
  const fileModifiedAt = formData.get("file_modified_at") as string | null;

  if (!file) {
    throw new Error("Asset data (file) is missing in form data.");
  }
  if (!deviceAssetId || !deviceId || !fileCreatedAt || !fileModifiedAt) {
    throw new Error("Required asset metadata is missing in form data.");
  }

  // Construct parameters for the SDK call
  const assetParams: Photos.AssetCreateParams = {
    asset_data: file, // The SDK likely expects a File/Blob object directly
    device_asset_id: deviceAssetId,
    device_id: deviceId,
    file_created_at: fileCreatedAt,
    file_modified_at: fileModifiedAt,
  };

  try {
    const newAsset = await photosClient.assets.create(assetParams);
    console.log(
      `[Action: createAssetAction] Success - Created asset ID: ${newAsset.id} (File: ${file.name})`
    );
    return newAsset;
  } catch (error) {
    console.error("Server Action Error (createAssetAction):", error);
    throw new Error("Failed to create asset via server action.");
  }
}

export async function retrieveAssetAction(
  assetId: string
): Promise<Photos.AssetResponse> {
  try {
    const asset = await photosClient.assets.retrieve(assetId);
    console.log(
      `[Action: retrieveAssetAction] Success - Retrieved asset ID: ${asset.id}`
    );
    return asset;
  } catch (error) {
    console.error(
      `Server Action Error (retrieveAssetAction ${assetId}):`,
      error
    );
    throw new Error(`Failed to retrieve asset ${assetId} via server action.`);
  }
}

// --- Search Action ---

export async function searchAssetsAction(
  params: Photos.SearchSearchParams
): Promise<Photos.AssetResponse[]> {
  if (!params.query) {
    throw new Error("Search query is required.");
  }
  try {
    const searchPage = await photosClient.search.search(params);
    const searchResults = (searchPage.data || []).map((item) => item.asset);

    console.log(
      `[Action: searchAssetsAction] Success - Found ${searchResults.length} assets for query: "${params.query}".`
    );
    return searchResults;
  } catch (error) {
    console.error(
      `Server Action Error (searchAssetsAction query="${params.query}"):`,
      error
    );
    throw new Error(
      `Failed to search assets for query "${params.query}" via server action.`
    );
  }
}