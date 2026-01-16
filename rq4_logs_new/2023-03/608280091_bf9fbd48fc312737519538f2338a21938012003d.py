import base64, semver, json, os, re, requests, requests.adapters, sys
from github import Github

def _get_all_from_image(image):
    # Examples of image:
    # flannel/flannel
    # quay.io/skopeo/stable
    # k8s.gcr.io/pause
    # ghcr.io/epinio/epinio-server
    # gcr.io/cloud-provider-vsphere/csi/release/syncer
    # registry.suse.com/bci/bci-busybox
    # dockerhub
    splitted_image = image.split("/")
    if len(splitted_image) == 2 and not '.' in splitted_image[0]:
        return "dockerhub", splitted_image[0], splitted_image[1]
    # registry.k8s.io/pause
    elif len(splitted_image) == 2 and '.' in splitted_image[0]:
        return splitted_image[0], "", splitted_image[1]
    # gcr.io/cloud-provider-vsphere/cpi/release/manager
    elif len(splitted_image) > 3 and '.' in splitted_image[0]:
        return splitted_image[0], splitted_image[1], "/".join(splitted_image[2:])
    else:
        return splitted_image[0], splitted_image[1], splitted_image[2]

def _get_image_tags(registry, namespace, package, nexturl, page=1):
    #print(f"registry({registry}),namespace({namespace}),package({package})")
    s = requests.Session()
    s.mount("https://", requests.adapters.HTTPAdapter(max_retries=requests.adapters.Retry(total=10, backoff_factor=1, status_forcelist=[ 500, 502, 503, 504 ])))

    # We set the variables/parameters based on the registry we need to query
    registry_url = ""
    params = {}
    headers = {}
    match registry:
        case "dockerhub":
            registry_url = f"https://registry.hub.docker.com/v2/namespaces/{namespace}/repositories/{package}/tags"
            params={"page": page, "page_size": 100}
        case "quay.io":
            registry_url = f"https://quay.io/api/v1/repository/{namespace}/{package}/tag/"
            params={"page": page, "page_size": 100}
        case "registry.k8s.io":
            if namespace != "":
                registry_url = f"https://registry.k8s.io/v2/{namespace}/{package}/tags/list"
            else:
                # Example: registry.k8s.io/pause
                registry_url = f"https://registry.k8s.io/v2/{package}/tags/list"
        case "registry.suse.com":
            # We need to request a token first
            base_registry_url = "https://registry.suse.com"
            scope = f"scope=repository:{namespace}/{package}:pull"
            token_url = f"{base_registry_url}/auth?service=SUSE+Linux+Docker+Registry"
            token_res = requests.get(url=token_url, params=scope)
            token_data = token_res.json()
            access_token = token_data['access_token']
            headers['Authorization'] = 'Bearer ' + access_token
            registry_url = f"{base_registry_url}/v2/{namespace}/{package}/tags/list"
        case "ghcr.io":
            ghcr_token = base64.b64encode(github_token.encode("utf-8")).decode("utf-8")
            headers['Authorization'] = 'Bearer ' + ghcr_token
            registry_url = f"https://ghcr.io/v2/{namespace}/{package}/tags/list"
            if nexturl != "":
                registry_url = f"https://ghcr.io{nexturl}"
        case "gcr.io":
            registry_url = f"https://gcr.io/v2/{namespace}/{package}/tags/list"
        case _:
            print(f"Unrecognized registry: {registry}")

    result = s.get(registry_url,
        params=params,
        headers=headers,
        timeout=10.0,
    )
    result.raise_for_status()
    data = result.json()
    # Return the correct data based on the registry we queried (or paginate if registry/query requires it)
    if registry == "dockerhub":
        tags = [tag["name"] for tag in data["results"]]
        print(f"tags for {namespace}/{package}: {tags}")
        if not data["next"]:
            return tags
        return tags + _get_image_tags(registry, namespace, package, "", page=page + 1)
    elif registry == "quay.io":
        tags = [tag["name"] for tag in data["tags"]]
        if not data["has_additional"]:
            return tags
        return tags + _get_image_tags(registry, namespace, package, "", page=page + 1)
    elif registry == "registry.k8s.io" or registry == "registry.suse.com" or registry == "gcr.io":
        return data["tags"]
    elif registry == "ghcr.io":
        tags = data["tags"]
        if not "next" in result.links:
            return tags
        return tags + _get_image_tags(registry, namespace, package, result.links["next"]["url"], page=page)
    else:
        print(f"Unrecognized registry: {registry}")

# using an access token
github_token = os.getenv('GITHUB_TOKEN')
g = Github(github_token)

# Configuration file with repositories
f = open('config.json', 'r')
data = json.load(f)
# Closing file
f.close()

# Read in images-list to check if image/tag already exist
textfile = open('../images-list', 'r')
filetext = textfile.read()
textfile.close()

# Function to search textfile content
def image_tag_already_exist(filetext, image, tag):
    matches = re.findall(fr"{image}\s.*?\s{tag}\n", filetext)
    if matches:
        return True
    return False

alldict = {}

# loop through each repository
for (key, values) in data.items():
    alldict[key] = {}
    # determine version source for the image tags
    repoString = values['versionSource'].split(":")
    if values['versionSource'] != "registry":
        if len(repoString) != 2:
            print(f"Can not extract proper version source from versionSource {values['versionSource']} for image {key}")
            sys.exit(1)

    found_releases = []

    match repoString[0]:
        case "github-releases":
            repo = g.get_repo(repoString[1])
            # Get all the releases used as source for the tag
            releases = repo.get_releases()
            if "versionConstraint" in values:
                constraint = values['versionConstraint']
            for release in releases:
                if not release.prerelease and not release.draft and semver.VersionInfo.isvalid(release.tag_name.removeprefix('v')):
                    if "versionFilter" in values:
                        if not re.search(values['versionFilter'], release.tag_name):
                            continue
                    if "versionConstraint" in values:
                        # Checking constraint
                        if not semver.match(release.tag_name.removeprefix('v'), constraint):
                            continue
                    found_releases.append(release.tag_name)
        case "github-latest-release":
            repo = g.get_repo(repoString[1])
            # Get all the releases used as source for the tag
            latest_release = repo.get_latest_release()
            found_releases.append(latest_release.tag_name)
        case "registry":
            registry, namespace, package  = _get_all_from_image(values['images'][0])
            image_tags = _get_image_tags(registry, namespace, package, "")
            found_images = []
            if "versionFilter" in values:
                for image_tag in image_tags:
                    if not re.search(values['versionFilter'], image_tag):
                        continue
                    found_images.append(image_tag)
            else:
                    found_images.extend(image_tags)
            if "latest" in values:
                found_images.sort(key = lambda x: [int(y.removeprefix('v')) for y in x.split('.')])
                found_releases = [found_images[-1]]
            else:
                found_releases.extend(found_images)
        case _:
            print(f"Version source {repoString[0]} is not supported")
            continue

    # deduplicate
    found_releases = set(found_releases)

    alldict[key]['images'] = []
    alldict[key]['tags'] = []

    for image in values['images']:
        for tag in found_releases:
            if not image_tag_already_exist(filetext, image, tag):
                if image not in alldict[key]['images']:
                    alldict[key]['images'].append(image)
                if tag not in alldict[key]['tags']:
                    alldict[key]['tags'].append(tag)
        # sort because we match existing pull requests on images/tags in pull request title
        alldict[key]['images'].sort()
        alldict[key]['tags'].sort()

# Print json
dictjson = json.dumps(alldict)
print(dictjson)