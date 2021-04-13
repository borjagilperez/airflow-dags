#!/bin/bash

PS3="Please select your choice: "
options=(
    "Init" \
    "Fetch" \
    "Pull" \
    "Push" \
    "Start feature" \
    "Finish feature" \
    "Start release/hotfix" \
    "Finish release/hotfix" \
    "Quit")

select opt in "${options[@]}"; do
    case $opt in
        "Init")
            read -p 'Username: ' USER_NAME
            git config user.name $USER_NAME
            read -p 'User email: ' USER_EMAIL
            git config user.email "$USER_EMAIL"

            git flow init

            break
            ;;

        "Fetch")
            git config credential.helper cache
            git fetch origin

            break
            ;;

        "Pull")
            git config credential.helper cache
            git pull origin

            break
            ;;

        "Push")
            git config credential.helper cache
            git push origin main
            git push origin --tags
            git push origin develop
            git remote prune origin

            break
            ;;

        "Start feature")
            read -p 'Name of the new feature: ' RESP
            git flow feature start $RESP
            git config credential.helper cache
            git flow feature publish

            break
            ;;

        "Finish feature")
            git config credential.helper cache
            git flow feature finish

            break
            ;;

        "Start release/hotfix")
            git status && echo ''
            read -p 'Have you committed the changes? [y/N]: ' resp_commit
            case $resp_commit in
                'y'|'Y')
                    read -p 'Start release or hotfix? [RELEASE/hotfix]: ' resp_branch
                    case $resp_branch in
                        "")
                            branch='release'
                            ;;
                        'release'|'RELEASE'|'hotfix'|'HOTFIX')
                            branch=$resp_branch
                            ;;
                        *)
                            echo "Invalid option"
                            
                            break
                            ;;
                    esac

                    eval "$($HOME/miniconda/bin/conda shell.bash hook)"
                    conda activate base && conda info --envs
                    echo 'Increment the <major> version when you make incompatible API changes.'
                    echo 'Increment the <minor> version when you add functionality in a backwards-compatible manner.'
                    echo 'Increment the <patch> version when you make backwards-compatible bug fixes.'
                    read -p 'Increment for repository: ' NAME

                    CURR_VERSION=$(cat ./VERSION | awk -F' ' 'NR==1{print $1}')
                    bumpversion --current-version $CURR_VERSION $NAME ./VERSION
                    NEW_VERSION=$(cat ./VERSION | awk -F' ' 'NR==1{print $1}')
                    git restore ./VERSION
                    git flow $branch start v$NEW_VERSION
                    bumpversion --current-version $CURR_VERSION $NAME ./VERSION
                    git add --all && git commit -m "$branch $NEW_VERSION"
                    ;;
                'n'|'N'|"")
                    echo 'Commit the changes first.'
                    ;;
                *)
                    echo "Invalid option"

                    break
                    ;;
            esac

            break
            ;;

        "Finish release/hotfix")
            git status && echo ''
            read -p 'Have you committed the changes? [y/N]: ' resp_commit
            case $resp_commit in
                'y'|'Y')
                    git flow $(git branch | grep '*' | awk -F' |/' 'NR==1{print $2}') finish
                    ;;
                'n'|'N'|"")
                    echo 'Commit the changes first.'
                    ;;
                *)
                    echo "Invalid option"
                    
                    break
                    ;;
            esac

            break
            ;;
            
        "Quit")
            break
            ;;
        *)
            echo "Invalid option"

            break
            ;;
    esac
done
