#!/bin/bash
# increment a version number for a build 
# and commit the contents of a repository 
# with that a version

# version file
# auto creates if non existant
version=source/version.txt

# min and max for each variable
# incase you want to limit range
MAJOR_MIN=1
MINOR_MIN=0
PATCH_MIN=0
MAJOR_MAX=999
MINOR_MAX=999
PATCH_MAX=999
MAJOR_INC=1
MINOR_INC=1
PATCH_INC=1


if [[ -f "$version" ]]; then
    # This gets the curent version.. and bumps the patch +1
    major=$(cat $version| cut -d '.' -f 1)
    minor=$(cat $version| cut -d '.' -f 2)
    patch=$(cat $version| cut -d '.' -f 3)
    # overflow logic
    patch=$((patch+PATCH_INC))
    if [[ $patch -gt $PATCH_MAX ]]; 
    then 
    patch=$PATCH_MIN
    minor=$((minor+MINOR_INC))
    fi
    if [[ $minor -gt $MINOR_MAX ]]; 
    then 
    minor=$MAJOR_MIN
    major=$((major+MAJOR_INC))
    fi
else
    major=$MAJOR_MIN
    minor=$MINOR_MIN
    patch=$PATCH_MIN
    DIR=$(dirname "${version}")
    mkdir -p "$DIR"
  
fi
build=$major.$minor.$patch

#put new version in text file
echo $build>$version
echo Curent Version: $build

# commit everything with build #
git add -A
git commit -m 'Commiting Build '$build
