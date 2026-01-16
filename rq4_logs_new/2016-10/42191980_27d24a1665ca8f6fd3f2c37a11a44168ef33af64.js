angular.module('app')
.controller('cuiTableCtrl', function($pagination, $state, $stateParams) {

	const cuiTable = this

	cuiTable.sortBy = {}
	cuiTable.page = parseInt($stateParams.page || 1)
	cuiTable.pageSize = parseInt($stateParams.pageSize || $pagination.getUserValue() || $pagination.getPaginationOptions[0])

	cuiTable.userList = [
		{
			name: {
				given: 'Steven',
				surname: 'Seagal'
			},
			status: 'Active',
			username: 'AkidoMaster1952'
		},
		{
			name: {
				given: 'Bill',
				surname: 'Murray'
			},
			status: 'Active',
			username: 'MysteriousMurray111'
		},
		{
			name: {
				given: 'Mr.',
				surname: 'Anderson'
			},
			status: 'Pending',
			username: 'Neo'
		}
	]

	// Helper Functions Start --------------------------------------------------

	const updateStateParams = (opts) => {
        $state.transitionTo('cuiTable', buildStateParams(opts), { notify:false })
    }

    const getSortNameByProperty = (property) => {
        switch (property) {
            case 'username':
            case 'status':
                return property
            case 'name.given':
                return 'name'
            case 'creation':
                return 'registered'
        }
    }

    const getSortPropertyByName = (name) => {
        switch (name) {
            case 'username':
            case 'status':
                return name
            case 'name':
                return 'name.given'
            case 'registered':
                return 'creation'
        }
    }

    const buildStateParams = ({ page, pageSize } = { page: cuiTable.page , pageSize: cuiTable.pageSize }) => {
        const params = {
            page,
            pageSize
        }
        let sortBy
        Object.keys(cuiTable.sortBy).forEach(key => {
            sortBy = key
        })
        if (sortBy) {
            params.sortBy = getSortNameByProperty(sortBy)
            params.sortType = cuiTable.sortBy[sortBy]
        }
        if ($stateParams.orgID) {
            params.orgID = $stateParams.orgID
        }
        return params
    }

	// Helper Functions End ----------------------------------------------------



	cuiTable.cuiTableOptions = {
		paginate: true,
		recordCount: 3,
		pageSize: 10,
		initialPage: 1,
		onPageChange: (page, pageSize) => {
    		updateStateParams({ page, pageSize })
		}
	}

	// cuiTable.sortBy = {
	// 	page: cuiTable.page,
	// 	pageSize: cuiTable.pageSize,
	// 	sortBy: 'name',
	// 	sortType: 'asc'
	// }

	cuiTable.sortingCallbacks = {
        name (opts) {
            cuiTable.sort('name.given', opts)
            updateStateParams()
        },
        username (opts) {
            cuiTable.sort('username', opts)
            updateStateParams()
        },
        registered (opts) {
            cuiTable.sort('creation', Object.assign({}, { mod: 'sortingInverted' }, opts))
            updateStateParams()
        },
        status (opts) {
            cuiTable.sort('status', opts)
            updateStateParams()
        }
    }

    cuiTable.sort = (type, opts) => {
        cuiTable.userList = DSLIDHelpers.sortUsers(cuiTable.unparsedUsers, type, cuiTable.sortBy, opts)
    }

	

})