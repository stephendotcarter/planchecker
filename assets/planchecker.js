function savePlan(){
    // Show spinner
    $('#saveSpinner').removeClass('hidden');
    $.ajax({
        method: "POST",
        url: "/plan/",
        dataType: "json",
        data: {
            action: "save",
            plantext: planTextBase64
        }
    }).done(function( res ) {
        if (res.status == "success") {
            $('#planRefLink').html(res.ref);
            $('#planRefLink').attr('href', '/plan/' + res.ref);
            $('#planRef').removeClass('hidden');
            $('#bookmarkMsg').removeClass('hidden');
            $('#planSave').addClass('hidden');
        } else if (res.status == "failure") {
            alert(res.msg);
        }

        // Remove spinner
        $('#saveSpinner').addClass('hidden');
    });
}

$(function () {
    // Initialize tooltips
    if ($('[data-toggle="tooltip"]').length > 0) {
        $('[data-toggle="tooltip"]').tooltip();
    }

    if (typeof planRef !== 'undefined') {
        if (planRef == "") {
            console.log("enable save");
            $('#planSave').removeClass('hidden');
            $('#alertTop').removeClass('hidden');
            $('#planRef').addClass('hidden');
        } else {
            console.log("enable ref");
            $('#planRef').removeClass('hidden');
            $('#planSave').addClass('hidden');
        }
    }
});
